// Consultas de análisis de grafos.
// Cubren productos comprados juntos, usuarios influyentes y rutas entre ubicaciones.

// 1. Los 5 productos más comprados juntos.
MATCH (a:Product)-[r:BOUGHT_TOGETHER]->(b:Product)
RETURN a.name AS producto_1,
       b.name AS producto_2,
       r.times AS veces_comprados_juntos
ORDER BY veces_comprados_juntos DESC, producto_1, producto_2
LIMIT 5;

// 2. Usuarios que recomiendan a otros.
MATCH (u:User)-[:RECOMMENDS]->(recommended:User)
RETURN u.user_id AS usuario_id,
       u.name AS usuario,
       count(recommended) AS usuarios_recomendados,
       collect(recommended.name) AS recomendados
ORDER BY usuarios_recomendados DESC, usuario;

// 3. Usuarios con mayor actividad de pedidos.
MATCH (u:User)-[:PLACED]->(o:Order)
RETURN u.user_id AS usuario_id,
       u.name AS usuario,
       count(o) AS pedidos_realizados
ORDER BY pedidos_realizados DESC, usuario
LIMIT 10;

// 4. Categorías más compradas según el grafo.
MATCH (:Order)-[r:CONTAINS]->(p:Product)
RETURN p.category AS categoria,
       sum(r.quantity) AS unidades,
       round(sum(r.line_total), 2) AS ingresos
ORDER BY ingresos DESC;

// 5. Camino de entrega con menor distancia acumulada desde el centro.
MATCH path = (:Location {location_id: 'loc-central'})-[:ROUTE_TO*1..8]->(dest:Location)
WITH path,
     dest,
     reduce(total = 0.0, rel IN relationships(path) | total + coalesce(rel.distance_km, 0.0)) AS distancia_total_km,
     reduce(tiempo = 0.0, rel IN relationships(path) | tiempo + coalesce(rel.estimated_minutes, 0.0)) AS tiempo_total_min
RETURN dest.name AS destino,
       [node IN nodes(path) | node.name] AS ruta,
       round(distancia_total_km, 3) AS distancia_total_km,
       round(tiempo_total_min, 2) AS tiempo_total_min
ORDER BY distancia_total_km ASC
LIMIT 5;
