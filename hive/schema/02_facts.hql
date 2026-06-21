-- =============================================================================
-- 02_facts.hql
-- Tablas de hechos del esquema estrella.
-- Ejecutar después de 01_dimensions.hql.
-- =============================================================================

USE restaurants_dw;

-- -----------------------------------------------------------------------------
-- fact_items_pedido
-- Grano: una fila por ítem dentro de un pedido (order_items del Proyecto 1).
-- Es la tabla de hechos principal — permite análisis de ingresos, productos,
-- categorías, tendencias y horarios pico.
--
-- Escrita por Spark: spark/jobs/cargar_fact_items_pedido.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_items_pedido (
  -- Llaves foráneas a dimensiones
  tiempo_key        BIGINT    COMMENT 'FK → dim_tiempo',
  usuario_key       BIGINT    COMMENT 'FK → dim_usuario',
  restaurante_key   BIGINT    COMMENT 'FK → dim_restaurante',
  producto_key      BIGINT    COMMENT 'FK → dim_producto',

  -- Claves naturales (para trazabilidad)
  pedido_id         STRING    COMMENT 'UUID del order en Proyecto 1',
  item_id           STRING    COMMENT 'UUID del order_item en Proyecto 1',

  -- Medidas
  cantidad          INT,
  precio_unitario   DECIMAL(10,2)   COMMENT 'Precio al momento del pedido (snapshot)',
  monto_total       DECIMAL(10,2)   COMMENT 'cantidad * precio_unitario',

  -- Atributos degenerados del pedido
  estado_pedido     STRING    COMMENT 'pending | confirmed | cancelled',
  es_para_llevar    BOOLEAN
)
COMMENT 'Fact table principal: ítems de pedidos'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- -----------------------------------------------------------------------------
-- fact_reservaciones
-- Grano: una fila por reservación (reservations del Proyecto 1).
-- Permite análisis de ocupación, cancelaciones y comportamiento de clientes.
--
-- Escrita por Spark: spark/jobs/cargar_fact_reservaciones.py
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_reservaciones (
  -- Llaves foráneas a dimensiones
  tiempo_key        BIGINT    COMMENT 'FK → dim_tiempo (fecha de la reserva)',
  usuario_key       BIGINT    COMMENT 'FK → dim_usuario',
  restaurante_key   BIGINT    COMMENT 'FK → dim_restaurante',

  -- Clave natural
  reservacion_id    STRING    COMMENT 'UUID de la reservación en Proyecto 1',

  -- Medidas
  tamano_grupo      INT       COMMENT 'party_size',

  -- Atributos degenerados
  estado            STRING    COMMENT 'pending | confirmed | cancelled'
)
COMMENT 'Fact table de reservaciones'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
