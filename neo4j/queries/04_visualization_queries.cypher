// Visualización general del grafo de usuarios, pedidos y productos.
MATCH p = (u:User)-[:PLACED]->(o:Order)-[:CONTAINS]->(prod:Product)
RETURN p
LIMIT 50;

// Visualización de productos comprados juntos.
MATCH p = (a:Product)-[:BOUGHT_TOGETHER]->(b:Product)
RETURN p
LIMIT 30;

// Visualización de recomendaciones entre usuarios.
MATCH p = (u:User)-[:RECOMMENDS]->(recommended:User)
RETURN p
LIMIT 30;

// Visualización de pedidos con ubicación de entrega.
MATCH p = (o:Order)-[:DELIVERS_TO]->(l:Location)
RETURN p
LIMIT 50;

// Visualización de rutas asignadas por repartidor.
MATCH p = (c:Courier)-[:HAS_STOP]->(s:RouteStop)-[:DELIVERS_ORDER]->(o:Order)-[:DELIVERS_TO]->(l:Location)
RETURN p
LIMIT 60;

// Visualización de tramos de ruta entre ubicaciones.
MATCH p = (:Location)-[:ROUTE_TO]->(:Location)
RETURN p
LIMIT 60;

// Camino desde el centro de distribución hacia ubicaciones de entrega.
MATCH p = (:Location {location_id: "loc-central"})-[:ROUTE_TO*1..5]->(:Location)
RETURN p
LIMIT 20;