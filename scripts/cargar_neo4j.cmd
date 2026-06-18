@echo off
setlocal

REM Carga del grafo Neo4J desde los CSV generados por Spark.
REM Ejecutar desde la raiz del repositorio Restaurants-analytics.

set NEO4J_CONTAINER=ra_neo4j
set NEO4J_USER=neo4j
set NEO4J_PASSWORD=Analytics2024!

echo [INFO] Verificando CSVs en neo4j\import...
if not exist neo4j\import\users.csv (
  echo [ERROR] No se encontraron CSVs en neo4j\import.
  echo Ejecute primero scripts\validar_spark_sample.cmd
  exit /b 1
)

echo [INFO] Aplicando constraints...
docker exec -i %NEO4J_CONTAINER% cypher-shell -u %NEO4J_USER% -p %NEO4J_PASSWORD% < neo4j\queries\00_constraints.cypher
if errorlevel 1 exit /b 1

echo [INFO] Cargando grafo...
docker exec -i %NEO4J_CONTAINER% cypher-shell -u %NEO4J_USER% -p %NEO4J_PASSWORD% < neo4j\queries\01_load_graph.cypher
if errorlevel 1 exit /b 1

echo [INFO] Ejecutando consultas de analisis...
docker exec -i %NEO4J_CONTAINER% cypher-shell -u %NEO4J_USER% -p %NEO4J_PASSWORD% < neo4j\queries\02_analysis_queries.cypher
if errorlevel 1 exit /b 1

echo [INFO] Ejecutando consultas de rutas...
docker exec -i %NEO4J_CONTAINER% cypher-shell -u %NEO4J_USER% -p %NEO4J_PASSWORD% < neo4j\queries\03_route_queries.cypher
if errorlevel 1 exit /b 1

echo.
echo [OK] Carga y validacion Neo4J finalizada.
endlocal
