# Restaurants Analytics

Stack de analítica OLAP para el Proyecto 2 del curso **Base de Datos 2 — TEC**.  
Extiende el [Proyecto 1 (Restaurants-e2)](https://github.com/c4dd3/Restaurants-e2) con Data Warehouse, procesamiento distribuido con Spark, orquestación con Airflow, análisis de grafos con Neo4J y dashboards con Metabase.

---

## Tabla de contenidos

- [Arquitectura](#arquitectura)
- [Servicios](#servicios)
- [Requisitos previos](#requisitos-previos)
- [Setup completo](#setup-completo-primera-vez-o-reset)
- [Bajar todo](#bajar-todo)
- [Comandos disponibles](#comandos-disponibles)
- [Acceso a las interfaces](#acceso-a-las-interfaces)
- [Pipeline ETL (Airflow DAG)](#pipeline-etl-airflow-dag)
- [Data Warehouse en Hive](#data-warehouse-en-hive)
- [Análisis de grafos en Neo4J](#análisis-de-grafos-en-neo4j)
- [Dashboards en Metabase](#dashboards-en-metabase)
- [Variables de entorno](#variables-de-entorno)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Solución de problemas](#solución-de-problemas)

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────┐
│                     Proyecto 1  —  re2_net                       │
│          Postgres          MongoDB          ElasticSearch        │
└────────────────────────────┬─────────────────────────────────────┘
                             │  red externa (RE2_NETWORK_NAME)
┌────────────────────────────▼─────────────────────────────────────┐
│                  Stack de Analítica  —  ra_net                   │
│                                                                  │
│   ┌──────────────────────┐        ┌───────────────────────────┐  │
│   │    Apache Airflow    │──────> │      Apache Spark         │  │
│   │    DAG: ETL pipeline │        │    Master  +  Worker      │  │
│   │    :8085             │        │    :8090 (UI)  /  :7077   │  │
│   └──────────────────────┘        └─────────────┬─────────────┘  │
│                                                 │                │
│                  ┌──────────────────────────────▼─────────────┐  │
│                  │               Apache Hive                  │  │
│                  │   Metastore (:9083) + HiveServer2 (:10000) │  │
│                  │   Data Warehouse — esquema estrella        │  │
│                  └──────────────┬─────────────────────────────┘  │
│                                 │                                │
│   ┌─────────────────────────────▼──────┐  ┌──────────────────┐   │
│   │             Metabase               │  │      Neo4J       │   │
│   │   Dashboards de visualización      │  │  Grafos y rutas  │   │
│   │   :3000                            │  │  :7474  /  :7687 │   │
│   └────────────────────────────────────┘  └──────────────────┘   │
│                                                                  │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │                      analytics-db                         │  │
│   │    Postgres:  hive_metastore  |  airflow  |  metabase     │  │
│   └───────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Servicios

| Servicio         | Imagen             | Puerto(s)    | Función                                            |
|------------------|--------------------|--------------|---------------------------------------------------|
| `analytics-db`   | postgres:16-alpine | —            | Metastore de Hive, metadata de Airflow y Metabase |
| `hive-metastore` | Dockerfile.hive    | 9083         | Catálogo del Data Warehouse (Thrift)              |
| `hive-server`    | Dockerfile.hive    | 10000, 10002 | HiveServer2 / Beeline                             |
| `spark-master`   | bitnami/spark:3.5  | 8090, 7077   | Coordinador de jobs Spark                         |
| `spark-worker`   | bitnami/spark:3.5  | —            | Ejecutor de transformaciones                      |
| `airflow`        | Dockerfile.airflow | 8085         | Orquestación del pipeline ETL                     |
| `neo4j`          | neo4j:5.20         | 7474, 7687   | Grafos de usuarios, productos y rutas             |
| `metabase`       | metabase/metabase  | 3000         | Dashboards OLAP                                   |

---

## Requisitos previos

- **Docker 24+** y **Docker Compose v2**
- **Make** (incluido en macOS y Linux)
- **Python 3** con `cryptography` y `psycopg2-binary` (`make setup` los instala automáticamente)
- El repo del **Proyecto 1** clonado en `../Restaurants-e2` (o indicar ruta con `P1=`)
- Mínimo **8 GB de RAM** disponibles para Docker

> **Windows:** `make` no está disponible nativamente. Tenés dos opciones:
> - **WSL2** (recomendado): instalá Ubuntu desde Microsoft Store y usá los comandos igual que en macOS/Linux.
> - **PowerShell nativo**: usá `setup.ps1` incluido en el repo (ver [Setup en Windows](#setup-en-windows-powershell)).

---

## Setup completo (primera vez o reset)

Un solo comando levanta todo desde cero: Proyecto 1, datos semilla, stack de analítica, ETL, Neo4J y Metabase.

```bash
git clone https://github.com/c4dd3/Restaurants-analytics
cd Restaurants-analytics
make setup
```

Si el Proyecto 1 está en una ruta diferente a `../Restaurants-e2`:

```bash
make setup P1=/ruta/al/proyecto1
```

El proceso completo tarda entre 15 y 25 minutos. Al finalizar imprime las URLs y credenciales de acceso.

`make demo-reset` es un alias exacto de `make setup` y produce el mismo resultado.

### ¿Qué hace `make setup`?

| Paso | Acción |
|------|--------|
| 1 | Baja el stack de analítica con volúmenes (limpieza total) |
| 2 | Baja el Proyecto 1 con volúmenes |
| 3 | Levanta el Proyecto 1 y espera que su API responda |
| 4 | Siembra datos base: 10 restaurantes, 2 menús c/u, 8 productos c/u, 20 usuarios |
| 5 | Siembra 300 órdenes y 150 reservaciones aleatorias |
| 6 | Configura `.env` automáticamente (red re2, llaves de Airflow) y levanta el stack de analítica |
| 7 | Crea el esquema Hive: dimensiones, tablas de hechos y vistas OLAP |
| 8 | Despausa y ejecuta el DAG de ETL en Airflow; espera hasta que termine (máx. 20 min) |
| 9 | Carga el grafo en Neo4J y configura conexiones y dashboards de Metabase |

---

## Setup en Windows (PowerShell)

Si no usás WSL2, el repo incluye `setup.ps1` que hace exactamente lo mismo que `make setup` pero en PowerShell nativo.

### Requisitos previos en Windows

- **Docker Desktop** con backend WSL2 habilitado
- **Python 3** instalado y en PATH ([python.org](https://www.python.org/downloads/windows/))
- **Go 1.21+** instalado y en PATH ([go.dev](https://go.dev/dl/))
- **PowerShell 5.1+** (incluido en Windows 10/11) o PowerShell 7+

### Primera ejecución: habilitar scripts

Por defecto Windows bloquea la ejecución de scripts `.ps1`. Abrí PowerShell **como administrador** y ejecutá una sola vez:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Correr el setup

```powershell
git clone https://github.com/c4dd3/Restaurants-analytics
cd Restaurants-analytics
.\setup.ps1
```

Si el Proyecto 1 está en una ruta diferente a `..\Restaurants-e2`:

```powershell
.\setup.ps1 -P1 "C:\ruta\al\Restaurants-e2"
```

El proceso tarda entre 15 y 25 minutos. Al finalizar imprime las mismas URLs y credenciales que `make setup`.

### Comandos equivalentes en PowerShell

El repo incluye `win.ps1`, que reemplaza todos los targets del Makefile:

```powershell
.\win.ps1 setup          # levanta todo desde cero
.\win.ps1 teardown       # baja P1 y P2 con volúmenes
.\win.ps1 up             # levanta solo el stack de analítica
.\win.ps1 down           # baja el stack conservando volúmenes
.\win.ps1 down-v         # baja el stack y elimina volúmenes
.\win.ps1 logs           # sigue los logs en tiempo real
.\win.ps1 ps             # estado de todos los contenedores
.\win.ps1 beeline        # abre Beeline en HiveServer2
.\win.ps1 neo4j-shell    # abre Cypher Shell en Neo4J
.\win.ps1 spark-shell    # abre Spark Shell
.\win.ps1 neo4j-load     # recarga el grafo desde los CSVs
.\win.ps1 neo4j-analysis # ejecuta las queries de análisis
.\win.ps1 spark-sample   # corre el job de Spark con datos de muestra
```

Si el Proyecto 1 está en una ruta diferente, pasá `-P1` igual que con `setup.ps1`:

```powershell
.\win.ps1 teardown -P1 "C:\ruta\al\Restaurants-e2"
```

---

## Bajar todo

Para detener y limpiar ambos proyectos completamente (incluyendo volúmenes):

```bash
make teardown
```

Para bajar solo el stack de analítica sin tocar el Proyecto 1:

```bash
make down      # conserva volúmenes
make down-v    # elimina volúmenes
```

---

## Comandos disponibles

```bash
# Setup y ciclo de vida completo
make setup             # levanta todo desde cero (primera vez o reset)
make demo-reset        # alias de make setup
make teardown          # baja Proyecto 1 y Proyecto 2 con volúmenes

# Stack de analítica
make up                # levanta solo el stack de analítica (sin tocar P1)
make down              # baja el stack conservando volúmenes
make down-v            # baja el stack y elimina volúmenes
make logs              # sigue los logs de todos los servicios en tiempo real
make ps                # muestra el estado de todos los contenedores

# Shells interactivos
make beeline           # abre Beeline conectado a HiveServer2
make neo4j-shell       # abre Cypher Shell en Neo4J
make spark-shell       # abre Spark Shell en el master

# Neo4J
make neo4j-load        # recarga constraints y grafo desde los CSVs
make neo4j-analysis    # ejecuta las queries de análisis de grafos

# Spark
make spark-job-sample  # corre el job de Spark con datos de muestra (sin Postgres)
```

---

## Acceso a las interfaces

| Interfaz        | URL                    | Credenciales                             |
|-----------------|------------------------|------------------------------------------|
| Airflow         | http://localhost:8085  | `admin` / `admin`                        |
| Spark Master UI | http://localhost:8090  | —                                        |
| HiveServer2 UI  | http://localhost:10002 | —                                        |
| Neo4J Browser   | http://localhost:7474  | `neo4j` / `Analytics2024!`               |
| Metabase        | http://localhost:3000  | `admin@restaurants.local` / `Admin1234!` |

---

## Pipeline ETL (Airflow DAG)

El DAG `restaurants_etl` corre automáticamente todos los días a las **2:00 AM** y puede dispararse manualmente desde la UI de Airflow.

### Flujo de tareas

```
verificar_fuentes
      │
      ▼
cargar_dimensiones  (Spark)
      │
      ├──▶  cargar_fact_items_pedido   (Spark) ─┐
      └──▶  cargar_fact_reservaciones  (Spark) ─┤
                                                 │
                                                 ▼
                                    verificar_cambios_productos
                                         │              │
                                         ▼              ▼
                               reindexar_elasticsearch  sin_cambios
                                         │              │
                                         └──────┬───────┘
                                                ▼
                                         pipeline_completo
```

### Descripción de cada tarea

**`verificar_fuentes`** — comprueba que Postgres del Proyecto 1 está accesible y tiene datos antes de iniciar el pipeline. Si falla, detiene el DAG limpiamente.

**`cargar_dimensiones`** — job Spark que lee usuarios, restaurantes y productos de Postgres y los escribe en Hive como `dim_usuario`, `dim_restaurante`, `dim_producto` y `dim_tiempo`.

**`cargar_fact_items_pedido`** — job Spark que une `orders` + `order_items` con las dimensiones y escribe `fact_items_pedido` en Hive.

**`cargar_fact_reservaciones`** — job Spark que transforma `reservations` y escribe `fact_reservaciones` en Hive. Corre en paralelo con la tarea anterior.

**`verificar_cambios_productos`** — calcula un hash MD5 del catálogo de productos y lo compara con el de la ejecución anterior (guardado como Variable de Airflow). Si el catálogo cambió, deriva a `reindexar_elasticsearch`; si no, a `sin_cambios`.

**`reindexar_elasticsearch`** — llama al endpoint `POST /search/reindex` del Proyecto 1 para actualizar el índice de búsqueda. Requiere la variable Airflow `api_admin_token`.

**`pipeline_completo`** — marcador de fin; corre independientemente de cuál rama del branch se ejecutó.

### Conexiones de Airflow requeridas

| ID de conexión      | Tipo     | Host            | Puerto | Schema      |
|---------------------|----------|-----------------|--------|-------------|
| `postgres_proyecto1`| Postgres | `re2_postgres`  | 5432   | `restaurants` |
| `spark_default`     | Spark    | `spark-master`  | 7077   | —           |

Estas conexiones se crean automáticamente al correr `make setup`.

---

## Data Warehouse en Hive

Base de datos: `restaurants_dw`

### Esquema estrella

```
                    ┌─────────────┐
                    │  dim_tiempo │
                    │  tiempo_key │
                    │  anio       │
                    │  mes        │
                    │  dia        │
                    │  hora       │
                    └──────┬──────┘
                           │
┌──────────────────┐ ┌─────▼──────────────────┐ ┌──────────────────┐
│   dim_usuario    │ │   fact_items_pedido    │ │   dim_producto   │
│   usuario_key    │─│   tiempo_key (FK)      │─│   producto_key   │
│   nombre         │ │   usuario_key (FK)     │ │   nombre         │
│   email          │ │   restaurante_key (FK) │ │   categoria      │
│   rol            │ │   producto_key (FK)    │ │   precio         │
└──────────────────┘ │   pedido_id            │ └──────────────────┘
                     │   item_id              │
                     │   cantidad             │
                     │   precio_unitario      │
                     │   monto_total          │
                     │   estado_pedido        │
                     │   es_para_llevar       │
                     └──────┬─────────────────┘
                            │
                    ┌───────▼──────────┐
                    │  dim_restaurante │
                    │  restaurante_key │
                    │  nombre          │
                    │  direccion       │
                    └──────────────────┘

          ┌──────────────────────────────────────┐
          │         fact_reservaciones           │
          │  tiempo_key, usuario_key,            │
          │  restaurante_key, reservacion_id,    │
          │  tamano_grupo, estado                │
          └──────────────────────────────────────┘
```

### Consultas de ejemplo con Beeline

```bash
make beeline
```

```sql
-- Ventas por restaurante
SELECT r.nombre, SUM(f.monto_total) AS ingresos
FROM restaurants_dw.fact_items_pedido f
JOIN restaurants_dw.dim_restaurante r ON f.restaurante_key = r.restaurante_key
GROUP BY r.nombre ORDER BY ingresos DESC;

-- Órdenes por mes
SELECT t.anio, t.mes, COUNT(DISTINCT f.pedido_id) AS ordenes
FROM restaurants_dw.fact_items_pedido f
JOIN restaurants_dw.dim_tiempo t ON f.tiempo_key = t.tiempo_key
GROUP BY t.anio, t.mes ORDER BY t.anio, t.mes;
```

---

## Análisis de grafos en Neo4J

El grafo modela usuarios, órdenes, productos, restaurantes y ubicaciones como nodos conectados por relaciones semánticas.

### Nodos y relaciones

| Nodo / Relación | Descripción |
|-----------------|-------------|
| `(User)-[:PLACED]->(Order)` | Un usuario realizó un pedido |
| `(Order)-[:CONTAINS]->(Product)` | Un pedido contiene un producto |
| `(Product)-[:BOUGHT_TOGETHER]->(Product)` | Dos productos comprados en el mismo pedido |
| `(User)-[:RECOMMENDS]->(User)` | Relación de influencia entre usuarios |
| `(Location)-[:ROUTE_TO]->(Location)` | Ruta de entrega entre ubicaciones |

### Consultas disponibles

```bash
make neo4j-analysis
```

Las queries en `neo4j/queries/02_analysis_queries.cypher` responden:

1. **Productos más comprados juntos** — co-ocurrencias en pedidos
2. **Usuarios influyentes** — quién recomienda a más personas
3. **Usuarios más activos** — top 10 por cantidad de pedidos
4. **Categorías más vendidas** — por unidades e ingresos
5. **Rutas de entrega más cortas** — camino de menor distancia desde el centro

### Ejemplo desde el browser (http://localhost:7474)

```cypher
// Productos frecuentemente comprados juntos
MATCH (a:Product)-[r:BOUGHT_TOGETHER]->(b:Product)
RETURN a.name, b.name, r.times
ORDER BY r.times DESC LIMIT 10;
```

---

## Dashboards en Metabase

Metabase se conecta a HiveServer2 vía SparkSQL y expone tres dashboards preconstruidos.

| Dashboard | Contenido |
|-----------|-----------|
| **Ventas** | Ingresos totales, órdenes por estado, productos más vendidos, ventas por hora |
| **Reservaciones** | Reservaciones por restaurante, distribución por tamaño de grupo, estado |
| **Operaciones** | Comparativa pickup vs. delivery, actividad por usuario, tendencia diaria |

Las preguntas y dashboards se configuran automáticamente vía API al final de `make setup`. Para reconfigurar manualmente:

```bash
python3 dashboards/metabase/setup_metabase.py
```

---

## Variables de entorno

El archivo `.env` se genera automáticamente al correr `make setup`. Para configurarlo manualmente:

```bash
cp .env.example .env
```

| Variable                | Descripción                                     | Default          |
|-------------------------|-------------------------------------------------|------------------|
| `RE2_NETWORK_NAME`      | Nombre de la red Docker del Proyecto 1          | auto-detectado   |
| `AIRFLOW_FERNET_KEY`    | Llave Fernet para cifrar conexiones de Airflow  | auto-generada    |
| `AIRFLOW_SECRET_KEY`    | Llave secreta para sesiones web de Airflow      | auto-generada    |
| `ANALYTICS_DB_USER`     | Usuario de analytics-db                         | `analytics`      |
| `ANALYTICS_DB_PASSWORD` | Contraseña de analytics-db                      | `analytics`      |
| `NEO4J_PASSWORD`        | Contraseña de Neo4J                             | `Analytics2024!` |
| `SPARK_WORKER_MEMORY`   | Memoria por worker de Spark                     | `2G`             |
| `SPARK_WORKER_CORES`    | Cores por worker de Spark                       | `2`              |

---

## Estructura del repositorio

```
Restaurants-analytics/
├── airflow/
│   ├── dags/
│   │   └── restaurants_etl.py       # Pipeline ETL orquestado
│   ├── plugins/
│   │   ├── extractors/              # Extracción desde Postgres y MongoDB
│   │   └── loaders/                 # Carga a Hive y ElasticSearch
│   └── requirements.txt
├── dashboards/
│   └── metabase/
│       └── setup_metabase.py        # Configura conexiones y dashboards via API
├── deployments/
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow           # Airflow + Java + JDBC driver pre-descargado
│   ├── Dockerfile.hive
│   ├── airflow/
│   │   └── entrypoint.sh            # Inicializa DB y arranca webserver + scheduler
│   └── postgres/
│       └── init-multiple-dbs.sh     # Crea hive_metastore, airflow y metabase
├── docs/                            # Documentación técnica y diagramas
├── hive/
│   └── schema/
│       ├── 01_dimensions.hql        # DDL de dimensiones
│       ├── 02_facts.hql             # DDL de tablas de hechos
│       └── 03_olap_views.hql        # Vistas OLAP precalculadas
├── neo4j/
│   ├── import/                      # CSVs generados por Spark para carga del grafo
│   └── queries/
│       ├── 00_constraints.cypher    # Índices y constraints
│       ├── 01_load_graph.cypher     # Carga inicial del grafo
│       └── 02_analysis_queries.cypher
├── scripts/
│   ├── configure_env.py             # Auto-genera llaves de Airflow y detecta red re2
│   └── seed_transactions.py         # Siembra órdenes y reservaciones en Proyecto 1
├── spark/
│   ├── conf/
│   │   ├── hive-site.xml            # Apunta Spark al metastore de Hive
│   │   └── spark-defaults.conf
│   ├── jobs/
│   │   ├── cargar_dimensiones.py
│   │   ├── cargar_fact_items_pedido.py
│   │   ├── cargar_fact_reservaciones.py
│   │   └── restaurants_spark_analytics.py  # Genera CSVs para Neo4J
│   └── utils/
├── .env.example
├── Makefile
└── README.md
```

---

## Solución de problemas

**El DAG falla en `cargar_dimensiones` con error de descarga de JAR**  
El driver JDBC de PostgreSQL se pre-descarga durante el build de la imagen. Si el error persiste, reconstruir el contenedor: `docker compose -f deployments/docker-compose.yml build airflow`.

**Airflow reinicia en bucle al arrancar**  
Generalmente porque las bases de datos `airflow` o `metabase` no existen aún. `make setup` las crea automáticamente. Si se levantó con `make up` directamente, crearlas manualmente:
```bash
docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE airflow;"
docker exec ra_analytics_db psql -U analytics -d hive_metastore -c "CREATE DATABASE metabase;"
docker restart ra_airflow
```

**`make setup` no encuentra la red re2**  
El Proyecto 1 debe estar levantado antes de correr `make setup`. Levantarlo primero:
```bash
cd ../Restaurants-e2
DB_ENGINE=postgres docker compose -f deployments/docker-compose.yml --profile postgres up -d
```

**Hive devuelve 0 filas después del ETL**  
Verificar que el volumen `hive_warehouse` esté montado en el contenedor de Airflow. Revisar `deployments/docker-compose.yml` y confirmar que el servicio `airflow` tiene el volumen `hive_warehouse:/opt/hive/data/warehouse`.

**Metabase no muestra datos**  
Verificar que HiveServer2 esté corriendo: `docker ps | grep hive`. Si el contenedor está healthy, la conexión en Metabase debe apuntar a `hive-server:10000` con driver SparkSQL.

---

## Créditos

Proyecto universitario — Instituto Tecnológico de Costa Rica, Base de Datos 2.
