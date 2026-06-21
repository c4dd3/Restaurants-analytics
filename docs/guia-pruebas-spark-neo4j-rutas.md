# Guía de pruebas: Spark, Neo4J y rutas

## 1. Levantar servicios necesarios

Desde la raíz del proyecto:

```cmd
docker compose -f deployments\docker-compose.yml up --build -d spark-master spark-worker neo4j analytics-db
```

Verificar contenedores:

```cmd
docker ps
```

Deben aparecer, como mínimo, `ra_spark_master`, `ra_spark_worker` y `ra_neo4j`.

## 2. Validar Spark

Ejecutar:

```cmd
scripts\validar_spark_sample.cmd
```

Esta prueba ejecuta el job `restaurants_spark_analytics.py` con datos sample y genera:

- tendencias de consumo,
- horarios pico,
- crecimiento mensual,
- resumen de reservas,
- asignaciones de rutas,
- CSVs para Neo4J.

Para revisar manualmente los resultados:

```cmd
docker exec -i ra_spark_master bash -lc "find /tmp/restaurants-output/results -maxdepth 3 -type f | sort"
```

Para revisar los CSVs de Neo4J:

```cmd
dir neo4j\import
```

## 3. Validar Neo4J

Después de generar los CSVs:

```cmd
scripts\cargar_neo4j.cmd
```

Este script ejecuta:

1. constraints,
2. carga del grafo,
3. consultas de análisis,
4. consultas de rutas.

## 4. Qué demostrar en video

Para Spark, mostrar que el job termina con `[OK]` y que existen carpetas de resultados. Explicar que el procesamiento se hace con DataFrames y SparkSQL.

Para Neo4J, mostrar que se cargan nodos de usuarios, productos, pedidos y ubicaciones. Luego mostrar consultas de co-compra, recomendaciones y caminos/rutas.

Para rutas, mostrar `route_assignments.csv` y las consultas de rutas por repartidor. Explicar que la asignación se calcula desde Spark con una heurística simple y luego se consulta como grafo en Neo4J.

## 5. Defensa técnica

Spark se usa para procesar y transformar datos porque permite trabajar con volúmenes grandes y producir resultados agregados. Neo4J se usa después para representar relaciones, como usuario-pedido-producto, recomendaciones y conexiones entre ubicaciones. Esta separación mantiene a Spark como motor de procesamiento y a Neo4J como motor de análisis relacional/grafos.
