-- =============================================================================
-- 03_olap_views.hql
-- Vistas OLAP sobre las tablas de hechos y dimensiones.
-- Ejecutar después de 01_dimensions.hql y 02_facts.hql.
--
-- Las vistas sirven tanto para Metabase (dashboards) como para análisis
-- ad-hoc con Beeline o SparkSQL.
-- =============================================================================

USE restaurants_dw;

-- -----------------------------------------------------------------------------
-- Vista 1: ingresos_por_mes_categoria
-- Satisface dashboard: "Ingresos por mes y categoría de producto"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_ingresos_por_mes_categoria AS
SELECT
  t.anio,
  t.mes,
  t.nombre_mes,
  p.categoria,
  COUNT(DISTINCT f.pedido_id)         AS total_pedidos,
  SUM(f.cantidad)                     AS unidades_vendidas,
  ROUND(SUM(f.monto_total), 2)        AS ingresos_totales,
  ROUND(AVG(f.monto_total), 2)        AS ticket_promedio
FROM fact_items_pedido f
JOIN dim_tiempo      t ON f.tiempo_key      = t.tiempo_key
JOIN dim_producto    p ON f.producto_key    = p.producto_key
WHERE f.estado_pedido = 'confirmed'
GROUP BY t.anio, t.mes, t.nombre_mes, p.categoria;

-- -----------------------------------------------------------------------------
-- Vista 2: actividad_usuarios_por_restaurante
-- Satisface dashboard: "Actividad de clientes por zona geográfica"
-- (se usa el restaurante como proxy de zona, ya que no hay lat/lng de cliente)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_actividad_usuarios_por_restaurante AS
SELECT
  r.nombre            AS restaurante,
  r.direccion         AS zona,
  t.anio,
  t.mes,
  t.nombre_mes,
  COUNT(DISTINCT f.usuario_key)           AS usuarios_unicos,
  COUNT(DISTINCT f.pedido_id)             AS total_pedidos,
  ROUND(SUM(f.monto_total), 2)            AS ingresos
FROM fact_items_pedido f
JOIN dim_tiempo       t ON f.tiempo_key      = t.tiempo_key
JOIN dim_restaurante  r ON f.restaurante_key = r.restaurante_key
WHERE f.estado_pedido = 'confirmed'
GROUP BY r.nombre, r.direccion, t.anio, t.mes, t.nombre_mes;

-- -----------------------------------------------------------------------------
-- Vista 3: estado_pedidos
-- Satisface dashboard: "Estadísticas de pedidos completados vs cancelados"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_estado_pedidos AS
SELECT
  t.anio,
  t.mes,
  t.nombre_mes,
  f.estado_pedido,
  COUNT(DISTINCT f.pedido_id)       AS total_pedidos,
  ROUND(SUM(f.monto_total), 2)      AS monto_total,
  ROUND(
    COUNT(DISTINCT f.pedido_id) * 100.0
    / SUM(COUNT(DISTINCT f.pedido_id)) OVER (PARTITION BY t.anio, t.mes),
    2
  )                                 AS porcentaje
FROM fact_items_pedido f
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
GROUP BY t.anio, t.mes, t.nombre_mes, f.estado_pedido;

-- -----------------------------------------------------------------------------
-- Vista 4: tendencias_consumo
-- Análisis de Spark #1 — productos y categorías más consumidas por mes
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_tendencias_consumo AS
SELECT
  t.anio,
  t.mes,
  t.nombre_mes,
  p.categoria,
  p.nombre                            AS producto,
  SUM(f.cantidad)                     AS unidades_vendidas,
  ROUND(SUM(f.monto_total), 2)        AS ingresos,
  RANK() OVER (
    PARTITION BY t.anio, t.mes, p.categoria
    ORDER BY SUM(f.cantidad) DESC
  )                                   AS ranking_en_categoria
FROM fact_items_pedido f
JOIN dim_tiempo   t ON f.tiempo_key   = t.tiempo_key
JOIN dim_producto p ON f.producto_key = p.producto_key
WHERE f.estado_pedido = 'confirmed'
GROUP BY t.anio, t.mes, t.nombre_mes, p.categoria, p.nombre;

-- -----------------------------------------------------------------------------
-- Vista 5: horarios_pico
-- Análisis de Spark #2 — horas con mayor volumen de pedidos
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_horarios_pico AS
SELECT
  t.hora,
  t.nombre_dia,
  t.es_fin_semana,
  COUNT(DISTINCT f.pedido_id)       AS total_pedidos,
  SUM(f.cantidad)                   AS unidades_vendidas,
  ROUND(SUM(f.monto_total), 2)      AS ingresos,
  ROUND(AVG(f.monto_total), 2)      AS ticket_promedio
FROM fact_items_pedido f
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
WHERE f.estado_pedido = 'confirmed'
GROUP BY t.hora, t.nombre_dia, t.es_fin_semana;

-- -----------------------------------------------------------------------------
-- Vista 6: crecimiento_mensual
-- Análisis de Spark #3 — crecimiento MoM de ingresos y pedidos
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_crecimiento_mensual AS
SELECT
  anio,
  mes,
  nombre_mes,
  ingresos_mes,
  total_pedidos,
  LAG(ingresos_mes) OVER (ORDER BY anio, mes)   AS ingresos_mes_anterior,
  ROUND(
    (ingresos_mes - LAG(ingresos_mes) OVER (ORDER BY anio, mes))
    / NULLIF(LAG(ingresos_mes) OVER (ORDER BY anio, mes), 0) * 100,
    2
  )                                              AS crecimiento_pct
FROM (
  SELECT
    t.anio,
    t.mes,
    t.nombre_mes,
    ROUND(SUM(f.monto_total), 2)       AS ingresos_mes,
    COUNT(DISTINCT f.pedido_id)        AS total_pedidos
  FROM fact_items_pedido f
  JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
  WHERE f.estado_pedido = 'confirmed'
  GROUP BY t.anio, t.mes, t.nombre_mes
) base;
