# Guía de operación manual

Paso a paso para levantar, operar y bajar el stack sin usar `make setup`.  
Útil para entender qué hace cada comando, depurar problemas o correr partes del pipeline de forma individual.

> Para el flujo automatizado completo, ver el [README](README.md) y usar `make setup`.

---

## Tabla de contenidos

- [Requisitos](#requisitos)
- [1. Configurar el entorno](#1-configurar-el-entorno)
- [2. Levantar el Proyecto 1](#2-levantar-el-proyecto-1)
- [3. Sembrar datos en el Proyecto 1](#3-sembrar-datos-en-el-proyecto-1)
- [4. Levantar el stack de analítica](#4-levantar-el-stack-de-analítica)
- [5. Configurar Airflow](#5-configurar-airflow)
- [6. Crear el esquema Hive](#6-crear-el-esquema-hive)
- [7. Ejecutar el DAG de ETL](#7-ejecutar-el-dag-de-etl)
- [8. Cargar el grafo Neo4J](#8-cargar-el-grafo-neo4j)
- [9. Configurar Metabase](#9-configurar-metabase)
- [Bajar todo](#bajar-todo)
- [Comandos de verificación](#comandos-de-verificación)
- [Conexiones de Airflow](#conexiones-de-airflow)

---

## Requisitos

- Docker 24+ y Docker Compose v2
- Python 3 con `psycopg2-binary` y `cryptography`
- Go (para el seed del Proyecto 1)
- El repo de Proyecto 1 en `../Restaurants-e2` (o ajustar las rutas)

> **Windows:** todos los comandos de este manual usan sintaxis bash (macOS/Linux/WSL2). Si usás PowerShell nativo, ver las equivalencias en la tabla de la sección [Windows en el README](README.md#setup-en-windows-powershell). Los comandos `docker exec` y `docker compose` son idénticos en ambas plataformas; las diferencias están en la redirección de stdin (`<`) y la expansión de variables (`$(...)`), que en PowerShell se reemplazan por `Get-Content` y `$(...)` respectivamente.

---

## 1. Configurar el entorno

### Crear el archivo .env

```bash
cp .env.example .env
```

### Detectar la red del Proyecto 1

Con el Proyecto 1 levantado, buscar el nombre exacto de la red:

```bash
docker network ls | grep re2
```

Editar `.env` y poner el nombre encontrado:

```
RE2_NETWORK_NAME=deployments_re2_net
```

### Generar las llaves de Airflow

```bash
# Fernet key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret key
python3 -c "import secrets; print(secrets.token_hex(32))"
```

Pegar los valores en `.env`:

```
AIRFLOW_FERNET_KEY=<valor generado>
AIRFLOW_SECRET_KEY=<valor generado>
```

O usar el script que lo hace automáticamente:

```bash
python3 scripts/configure_env.py
```

---

## 2. Levantar el Proyecto 1

```bash
cd ../Restaurants-e2
DB_ENGINE=postgres docker compose -f deployments/docker-compose.yml --profile postgres up --build -d
```

Verificar que la API responde:

```bash
curl -s http://localhost/api/health
```

Esperar hasta obtener respuesta antes de continuar.

---

## 3. Sembrar datos en el Proyecto 1

### Datos base (restaurantes, menús, usuarios)

```bash
cd ../Restaurants-e2
go run ./scripts/seed -restaurants=10 -menus-per=2 -products-per=8 -users=20
```

### Transacciones (órdenes y reservaciones)

Desde la raíz de este repo:

```bash
pip3 install psycopg2-binary --break-system-packages
python3 scripts/seed_transactions.py --orders 300 --reservations 150
```

Parámetros opcionales:

| Flag | Default | Descripción |
|------|---------|-------------|
| `--orders` | 300 | Número de órdenes a generar |
| `--reservations` | 150 | Número de reservaciones a generar |

---

## 4. Levantar el stack de analítica

```bash
docker compose -f deployments/docker-compose.yml --env-file .env up --build -d
```

### Esperar a que analytics-db esté lista

```bash
until docker exec ra_analytics_db pg_isready -U analytics; do
  echo "Esperando analytics-db..."; sleep 3
done
```

### Crear las bases de datos requeridas

analytics-db arranca con `hive_metastore` como base por defecto. Airflow y Metabase necesitan sus propias bases:

```bash
docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE airflow;"
docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE metabase;"
```

### Reiniciar Airflow para que tome las bases recién creadas

```bash
docker restart ra_airflow
```

### Esperar a que Airflow esté listo

```bash
until docker exec ra_airflow airflow db check 2>/dev/null; do
  echo "Esperando Airflow..."; sleep 5
done
```

---

## 5. Configurar Airflow

### Crear las conexiones requeridas

```bash
# Conexión a Postgres del Proyecto 1
docker exec ra_airflow airflow connections add postgres_proyecto1 \
  --conn-type postgres \
  --conn-host re2_postgres \
  --conn-port 5432 \
  --conn-schema restaurants \
  --conn-login postgres \
  --conn-password postgres

# Conexión al Spark Master
docker exec ra_airflow airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077
```

### Verificar conexiones creadas

```bash
docker exec ra_airflow airflow connections list
```

### (Opcional) Token de admin para reindexar ElasticSearch

Si el catálogo de productos cambia, el DAG llama a `POST /search/reindex`. Requiere el JWT de un usuario admin del Proyecto 1:

```bash
# Obtener el token
curl -s -X POST http://localhost/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"tu_password"}' | jq -r '.token'

# Guardarlo en Airflow
docker exec ra_airflow airflow variables set api_admin_token <TOKEN>
```

---

## 6. Crear el esquema Hive

Crear la base de datos y las tablas del Data Warehouse en orden:

```bash
# Dimensiones
docker exec -i ra_hive_server beeline \
  -u "jdbc:hive2://localhost:10000" -n root --silent=true \
  < hive/schema/01_dimensions.hql

# Tablas de hechos
docker exec -i ra_hive_server beeline \
  -u "jdbc:hive2://localhost:10000" -n root --silent=true \
  < hive/schema/02_facts.hql

# Vistas OLAP
docker exec -i ra_hive_server beeline \
  -u "jdbc:hive2://localhost:10000" -n root --silent=true \
  < hive/schema/03_olap_views.hql
```

### Verificar desde Beeline

```bash
docker exec -it ra_hive_server beeline -u jdbc:hive2://localhost:10000
```

```sql
SHOW DATABASES;
USE restaurants_dw;
SHOW TABLES;
```

---

## 7. Ejecutar el DAG de ETL

### Esperar a que Airflow esté healthy

```bash
until [ "$(docker inspect --format='{{.State.Health.Status}}' ra_airflow)" = "healthy" ]; do
  echo "Airflow no está healthy aún..."; sleep 15
done
echo "Airflow healthy"
```

### Despausar y ejecutar el DAG

```bash
docker exec ra_airflow airflow dags unpause restaurants_etl
docker exec ra_airflow airflow dags trigger restaurants_etl
```

### Monitorear la ejecución

Desde la UI: http://localhost:8085 → DAG `restaurants_etl`

O desde la CLI:

```bash
# Ver el estado del último run
docker exec ra_airflow airflow dags list-runs -d restaurants_etl --output table

# Ver logs de una tarea específica
docker exec ra_airflow airflow tasks logs restaurants_etl cargar_dimensiones <run_id>
```

### Verificar datos en Hive tras el ETL

```bash
docker exec ra_hive_server beeline -u "jdbc:hive2://localhost:10000" \
  -n root --silent=true \
  -e "SELECT 'fact_items_pedido', COUNT(*) FROM restaurants_dw.fact_items_pedido
      UNION ALL SELECT 'fact_reservaciones', COUNT(*) FROM restaurants_dw.fact_reservaciones
      UNION ALL SELECT 'dim_usuario', COUNT(*) FROM restaurants_dw.dim_usuario;" \
  2>&1 | grep -v SLF4J
```

---

## 8. Cargar el grafo Neo4J

### Paso 1: Generar los CSVs con Spark

```bash
docker exec ra_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  /opt/spark-apps/jobs/restaurants_spark_analytics.py --source postgres
```

### Paso 2: Cargar constraints y datos en Neo4J

```bash
NEO4J_PWD=$(grep NEO4J_PASSWORD .env | cut -d= -f2)

# Constraints e índices
docker exec -i ra_neo4j cypher-shell -u neo4j -p "$NEO4J_PWD" \
  < neo4j/queries/00_constraints.cypher

# Nodos y relaciones
docker exec -i ra_neo4j cypher-shell -u neo4j -p "$NEO4J_PWD" \
  < neo4j/queries/01_load_graph.cypher
```

### Paso 3: Ejecutar las consultas de análisis

```bash
docker exec -i ra_neo4j cypher-shell -u neo4j -p "$NEO4J_PWD" \
  < neo4j/queries/02_analysis_queries.cypher
```

O acceder al browser: http://localhost:7474 (`neo4j` / `Analytics2024!`)

---

## 9. Configurar Metabase

```bash
python3 dashboards/metabase/setup_metabase.py
```

El script crea automáticamente:
- La conexión a HiveServer2 vía SparkSQL
- Las preguntas (queries) sobre el DW
- Los dashboards de Ventas, Reservaciones y Operaciones

Acceder en: http://localhost:3000 (`admin@restaurants.local` / `Admin1234!`)

---

## Bajar todo

### Solo el stack de analítica (conservando volúmenes)

```bash
docker compose -f deployments/docker-compose.yml --env-file .env down
```

### Stack de analítica con volúmenes (reset completo del P2)

```bash
docker compose -f deployments/docker-compose.yml --env-file .env down -v
```

### Proyecto 1 con volúmenes

```bash
cd ../Restaurants-e2
docker compose -f deployments/docker-compose.yml --profile postgres down -v
```

### Todo junto (P1 + P2 con volúmenes)

```bash
docker compose -f deployments/docker-compose.yml --env-file .env down -v
cd ../Restaurants-e2 && docker compose -f deployments/docker-compose.yml --profile postgres down -v
```

---

## Comandos de verificación

```bash
# Estado de todos los contenedores
docker compose -f deployments/docker-compose.yml --env-file .env ps

# Logs de un servicio específico
docker compose -f deployments/docker-compose.yml --env-file .env logs -f airflow

# Shell interactivo de Hive
docker exec -it ra_hive_server beeline -u jdbc:hive2://localhost:10000

# Shell de Neo4J
docker exec -it ra_neo4j cypher-shell -u neo4j -p $(grep NEO4J_PASSWORD .env | cut -d= -f2)

# Shell de Spark
docker exec -it ra_spark_master /opt/spark/bin/spark-shell \
  --master spark://spark-master:7077

# Verificar que analytics-db tiene las 3 bases
docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "\l"
```

---

## Equivalencias PowerShell (Windows sin WSL2)

Los comandos `docker exec` son idénticos en Windows. Lo que cambia es la redirección de stdin y la expansión de variables del shell.

### Redirigir archivos a docker exec

```bash
# bash
docker exec -i ra_hive_server beeline ... < hive/schema/01_dimensions.hql
```

```powershell
# PowerShell
Get-Content hive\schema\01_dimensions.hql | docker exec -i ra_hive_server beeline ...
```

### Extraer valor del .env

```bash
# bash
NEO4J_PWD=$(grep NEO4J_PASSWORD .env | cut -d= -f2)
```

```powershell
# PowerShell
$NEO4J_PWD = (Get-Content .env | Where-Object { $_ -match '^NEO4J_PASSWORD=' }) -replace '^NEO4J_PASSWORD=',''
```

### Loops de espera

```bash
# bash
until docker exec ra_analytics_db pg_isready -U analytics; do
  sleep 3
done
```

```powershell
# PowerShell
while ($true) {
    docker exec ra_analytics_db pg_isready -U analytics 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 3
}
```

### Levantar el Proyecto 1 con variable de entorno

```bash
# bash
DB_ENGINE=postgres docker compose -f deployments/docker-compose.yml --profile postgres up -d
```

```powershell
# PowerShell
$env:DB_ENGINE = "postgres"
docker compose -f deployments/docker-compose.yml --profile postgres up -d
Remove-Item Env:\DB_ENGINE
```

### Cargar grafo Neo4J (pasos 8.2)

```powershell
$NEO4J_PWD = (Get-Content .env | Where-Object { $_ -match '^NEO4J_PASSWORD=' }) -replace '^NEO4J_PASSWORD=',''
Get-Content neo4j\queries\00_constraints.cypher | docker exec -i ra_neo4j cypher-shell -u neo4j -p $NEO4J_PWD
Get-Content neo4j\queries\01_load_graph.cypher  | docker exec -i ra_neo4j cypher-shell -u neo4j -p $NEO4J_PWD
```

---

## Conexiones de Airflow

Referencia completa de las conexiones que deben existir en Airflow para que el DAG funcione.

| ID | Tipo | Host | Puerto | Schema | Usuario | Contraseña |
|----|------|------|--------|--------|---------|------------|
| `postgres_proyecto1` | Postgres | `re2_postgres` | 5432 | `restaurants` | `postgres` | `postgres` |
| `spark_default` | Spark | `spark://spark-master` | 7077 | — | — | — |

Variables opcionales:

| Variable | Descripción |
|----------|-------------|
| `api_admin_token` | JWT de admin del Proyecto 1. Requerido si el catálogo de productos cambia. |
| `products_catalog_hash` | Generada automáticamente en la primera ejecución del DAG. No crear manualmente. |
