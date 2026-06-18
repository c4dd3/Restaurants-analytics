// Restricciones base para evitar nodos duplicados en el grafo.
// Este archivo se ejecuta antes de cargar los CSV generados por Spark.

CREATE CONSTRAINT user_id_unique IF NOT EXISTS
FOR (u:User)
REQUIRE u.user_id IS UNIQUE;

CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product)
REQUIRE p.product_id IS UNIQUE;

CREATE CONSTRAINT order_id_unique IF NOT EXISTS
FOR (o:Order)
REQUIRE o.order_id IS UNIQUE;

CREATE CONSTRAINT courier_id_unique IF NOT EXISTS
FOR (c:Courier)
REQUIRE c.courier_id IS UNIQUE;

CREATE CONSTRAINT location_id_unique IF NOT EXISTS
FOR (l:Location)
REQUIRE l.location_id IS UNIQUE;

CREATE CONSTRAINT route_stop_unique IF NOT EXISTS
FOR (s:RouteStop)
REQUIRE (s.courier_id, s.stop_order) IS UNIQUE;
