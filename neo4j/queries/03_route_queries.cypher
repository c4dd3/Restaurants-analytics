// Consultas específicas para asignación de rutas de entrega.

// Ruta ordenada por repartidor.
MATCH (c:Courier)-[:HAS_STOP]->(s:RouteStop)-[:DELIVERS_ORDER]->(o:Order)-[:DELIVERS_TO]->(l:Location)
RETURN c.courier_id AS repartidor,
       s.stop_order AS parada,
       o.order_id AS pedido,
       l.name AS ubicacion,
       s.zone AS zona,
       s.distance_from_previous_km AS distancia_desde_anterior_km,
       s.estimated_minutes AS tiempo_estimado_min,
       s.accumulated_km AS distancia_acumulada_km
ORDER BY repartidor, parada;

// Resumen de carga por repartidor.
MATCH (c:Courier)-[:HAS_STOP]->(s:RouteStop)
RETURN c.courier_id AS repartidor,
       count(s) AS pedidos_asignados,
       round(sum(s.distance_from_previous_km), 3) AS distancia_total_km,
       round(sum(s.estimated_minutes), 2) AS tiempo_total_min
ORDER BY repartidor;

// Tramos de ruta entre geonodos.
MATCH (fromLocation:Location)-[r:ROUTE_TO]->(toLocation:Location)
RETURN r.courier_id AS repartidor,
       r.stop_order AS parada,
       fromLocation.name AS desde,
       toLocation.name AS hacia,
       r.distance_km AS distancia_km,
       r.estimated_minutes AS tiempo_min
ORDER BY repartidor, parada;
