@echo off
setlocal enabledelayedexpansion

REM Validacion rapida del job Spark usando datos sample.
REM Ejecutar desde la raiz del repositorio Restaurants-analytics.

set SPARK_CONTAINER=ra_spark_master
set OUTPUT_DIR=/tmp/restaurants-output
set NEO4J_IMPORT=/opt/neo4j-import
set IVY_DIR=/tmp/.ivy2

where docker >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Docker no esta disponible en PATH.
  exit /b 1
)

echo [INFO] Preparando carpetas temporales dentro de Spark...
docker exec -i %SPARK_CONTAINER% bash -lc "mkdir -p %IVY_DIR% %OUTPUT_DIR% %NEO4J_IMPORT% && chmod -R 777 %IVY_DIR% %OUTPUT_DIR%"
if errorlevel 1 exit /b 1

echo [INFO] Ejecutando job Spark con datos sample...
docker exec -i %SPARK_CONTAINER% /opt/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=%IVY_DIR% /opt/spark-apps/jobs/restaurants_spark_analytics.py --source sample --output-base %OUTPUT_DIR% --neo4j-output %NEO4J_IMPORT% --couriers 2
if errorlevel 1 exit /b 1

echo.
echo [INFO] Resultados analiticos generados:
docker exec -i %SPARK_CONTAINER% bash -lc "find %OUTPUT_DIR%/results -maxdepth 3 -type f | sort"
if errorlevel 1 exit /b 1

echo.
echo [INFO] CSVs generados para Neo4J:
docker exec -i %SPARK_CONTAINER% bash -lc "find %NEO4J_IMPORT% -maxdepth 1 -type f -name '*.csv' | sort"
if errorlevel 1 exit /b 1

echo.
echo [OK] Validacion Spark finalizada.
endlocal
