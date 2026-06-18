// Carga del grafo desde CSVs generados por Spark.
// Requiere que primero se ejecute spark/jobs/restaurants_spark_analytics.py
// con --neo4j-output /opt/neo4j-import.

LOAD CSV WITH HEADERS FROM 'file:///users.csv' AS row
MERGE (u:User {user_id: row.user_id})
SET u.name = row.name,
    u.zone = row.zone;

LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
MERGE (p:Product {product_id: row.product_id})
SET p.name = row.name,
    p.category = row.category,
    p.price = toFloat(row.price);

LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row
MERGE (o:Order {order_id: row.order_id})
SET o.restaurant_id = row.restaurant_id,
    o.created_at = row.created_at,
    o.status = row.status,
    o.zone = row.zone
WITH row, o
MATCH (u:User {user_id: row.user_id})
MERGE (u)-[:PLACED]->(o);

LOAD CSV WITH HEADERS FROM 'file:///order_items.csv' AS row
MATCH (o:Order {order_id: row.order_id})
MATCH (p:Product {product_id: row.product_id})
MERGE (o)-[r:CONTAINS]->(p)
SET r.quantity = toInteger(row.quantity),
    r.unit_price = toFloat(row.unit_price),
    r.line_total = toInteger(row.quantity) * toFloat(row.unit_price);

LOAD CSV WITH HEADERS FROM 'file:///recommendations.csv' AS row
MATCH (fromUser:User {user_id: row.from_user_id})
MATCH (toUser:User {user_id: row.to_user_id})
MERGE (fromUser)-[:RECOMMENDS]->(toUser);

LOAD CSV WITH HEADERS FROM 'file:///co_purchases.csv' AS row
MATCH (a:Product {product_id: row.product_a_id})
MATCH (b:Product {product_id: row.product_b_id})
MERGE (a)-[r:BOUGHT_TOGETHER]->(b)
SET r.times = toInteger(row.times_bought_together);

LOAD CSV WITH HEADERS FROM 'file:///locations.csv' AS row
MERGE (l:Location {location_id: row.location_id})
SET l.name = row.name,
    l.latitude = toFloat(row.latitude),
    l.longitude = toFloat(row.longitude);

LOAD CSV WITH HEADERS FROM 'file:///route_assignments.csv' AS row
MERGE (c:Courier {courier_id: row.courier_id})
WITH row, c
MATCH (o:Order {order_id: row.order_id})
MATCH (l:Location {location_id: row.location_id})
MERGE (s:RouteStop {courier_id: row.courier_id, stop_order: toInteger(row.stop_order)})
SET s.zone = row.zone,
    s.distance_from_previous_km = toFloat(row.distance_from_previous_km),
    s.estimated_minutes = toFloat(row.estimated_minutes),
    s.accumulated_km = toFloat(row.accumulated_km)
MERGE (c)-[:HAS_STOP]->(s)
MERGE (s)-[:DELIVERS_ORDER]->(o)
MERGE (o)-[:DELIVERS_TO]->(l);

LOAD CSV WITH HEADERS FROM 'file:///route_edges.csv' AS row
MATCH (fromLocation:Location {location_id: row.from_location_id})
MATCH (toLocation:Location {location_id: row.to_location_id})
MERGE (fromLocation)-[r:ROUTE_TO {courier_id: row.courier_id, stop_order: toInteger(row.stop_order)}]->(toLocation)
SET r.distance_km = toFloat(row.distance_from_previous_km),
    r.estimated_minutes = toFloat(row.estimated_minutes);
