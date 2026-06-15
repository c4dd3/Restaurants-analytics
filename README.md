# Restaurants-analytics

Stack de analítica OLAP para el Proyecto 2 del curso Base de Datos 2 (TEC).
Extiende el [Proyecto 1 (Restaurants-e2)](https://github.com/c4dd3/Restaurants-e2) con capacidades de Data Warehouse, procesamiento con Spark, orquestación con Airflow, análisis de grafos con Neo4J y dashboards con Metabase.

## Arquitectura

```
                         ┌──────────────────────────────────────────┐
                         │         Proyecto 1 — re2_net             │
                         │   Postgres  │  MongoDB  │  ElasticSearch  │
                         └─────────────┬──────────────────────────── ┘
                                       │  (red externa re2_net)
                    ┌──────────────────▼─────────────────────────────┐
                    │           Stack de Analítica — ra_net           │
                    │                                                  │
     ┌──────────────▼──────────┐      ┌────────────────────────────┐  │
     │    Apache Airflow       │─────▶│     Apache Spark           │  │
     │  DAG: ETL pipeline      │      │  Master + Worker           │  │
     │  :8085                  │      │  :8090 (UI) / :7077        │  │
     └─────────────────────────┘      └──────────┬─────────────────┘  │
                                                  │                    │
                    ┌─────────────────────────────▼──────────────┐    │
                    │              Apache Hive                    │    │
                    │   Metastore (:9083) + HiveServer2 (:10000) │    │
                    │   Data Warehouse — esquema estrella         │    │
                    └────────────────┬────────────────────────────┘    │
                                     │                                  │
          ┌──────────────────────────▼──────┐  ┌─────────────────────┐ │
          │           Metabase              │  │       Neo4J         │ │
          │  Dashboards de visualización    │  │  Grafos y rutas     │ │
          │  :3000                          │  │  :7474 / :7687      │ │
          └─────────────────────────────────┘  └─────────────────────┘ │
                                                                        │
          ┌─────────────────────────────────────────────────────────┐  │
          │                    analytics-db                          │  │
          │  Postgres interno: hive_metastore | airflow | metabase  │  │
          └─────────────────────────────────────────────────────────┘  │
                    └──────────────────────────────────────────────────┘
```

## Componentes

| Servicio | Imagen | Puerto(s) | Función |
|---|---|---|---|
| `analytics-db` | postgres:16-alpine | — | Metastore de Hive, metadata de Airflow y Metabase |
| `hive-metastore` | apache/hive:4.0.0 | 9083 | Catálogo del Data Warehouse |
| `hive-server` | apache/hive:4.0.0 | 10000, 10002 | HiveServer2 / Beeline |
| `spark-master` | bitnami/spark:3.5 | 8090, 7077 | Coordinador de jobs Spark |
| `spark-worker` | bitnami/spark:3.5 | — | Ejecutor de transformaciones |
| `airflow` | apache/airflow:2.9.3 | 8085 | Orquestación del pipeline ETL |
| `neo4j` | neo4j:5.20 | 7474, 7687 | Grafos de usuarios/productos/rutas |
| `metabase` | metabase/metabase:v0.50.0 | 3000 | Dashboards OLAP |

## Requisitos previos

- Docker 24+ y Docker Compose v2
- El stack del **Proyecto 1** levantado (necesario para que exista la red `re2_net`)
- Al menos 8 GB de RAM disponibles para Docker

## Setup

### 1. Clonar y configurar variables

```bash
git clone <este-repo>
cd Restaurants-analytics
cp .env.example .env
```

Editar `.env` y completar:

```bash
# Generar Fernet key para Airflow
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Generar Secret key para Airflow
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 2. Levantar el Proyecto 1 primero

```bash
# En el directorio de Restaurants-e2:
DB_ENGINE=postgres docker compose -f deployments/docker-compose.yml --profile postgres up -d
# Esperar que esté healthy (~60s)
```

### 3. Levantar el stack de analítica

```bash
docker compose -f deployments/docker-compose.yml up --build -d
```

El primer arranque puede tardar 2-3 minutos mientras Hive inicializa el metastore y Airflow migra su base de datos.

### 4. Verificar servicios

```bash
# Estado de todos los contenedores
docker compose -f deployments/docker-compose.yml ps

# Logs de un servicio específico
docker compose -f deployments/docker-compose.yml logs -f airflow
```

### 5. Acceder a las UIs

| Interfaz | URL | Credenciales |
|---|---|---|
| Airflow | http://localhost:8085 | admin / admin |
| Spark Master | http://localhost:8090 | — |
| HiveServer2 | http://localhost:10002 | — |
| Neo4J Browser | http://localhost:7474 | neo4j / (NEO4J_PASSWORD del .env) |
| Metabase | http://localhost:3000 | Configurar en primer acceso |

### 6. Conectar Beeline a Hive (opcional)

```bash
docker exec -it ra_hive_server beeline -u jdbc:hive2://localhost:10000
```

### 7. Bajar el stack

```bash
docker compose -f deployments/docker-compose.yml down

# Bajar y borrar volúmenes (limpieza total)
docker compose -f deployments/docker-compose.yml down -v
```

## Estructura del repositorio

```
Restaurants-analytics/
├── airflow/
│   ├── dags/              # DAGs de Airflow (pipeline ETL)
│   └── plugins/
│       ├── extractors/    # Operadores de extracción (Postgres, Mongo)
│       └── loaders/       # Operadores de carga (Hive, ElasticSearch)
├── dashboards/
│   └── metabase/
│       └── exports/       # Dashboards exportados (.json)
├── deployments/
│   ├── docker-compose.yml # Stack completo de analítica
│   └── postgres/
│       └── init-multiple-dbs.sh
├── docs/                  # Documentación técnica y diagramas
├── hive/
│   └── schema/            # DDL del esquema estrella (HQL)
├── neo4j/
│   ├── import/            # CSVs para carga inicial del grafo
│   └── queries/           # Consultas Cypher
├── spark/
│   ├── jobs/              # Scripts PySpark por análisis
│   └── utils/             # Funciones compartidas
├── .env.example
└── README.md
```

## Flujo de datos

```
Postgres / MongoDB (Proyecto 1)
        │
        │  Extracción (Airflow + operadores custom)
        ▼
Apache Spark  ──→  Transformaciones (DataFrames / SparkSQL)
        │
        │  Carga
        ▼
Apache Hive  ──→  Data Warehouse (esquema estrella)
        │
        ├──→  Metabase  ──→  Dashboards
        │
        └──→  ElasticSearch  (reindexado si cambia catálogo)

MongoDB / Postgres
        │
        │  Importación
        ▼
Neo4J  ──→  Grafos (co-compras, usuarios influyentes, rutas de reparto)
```

## Créditos

Proyecto universitario — Tecnológico de Costa Rica, Base de Datos 2.
Profesor: Kenneth Obando Rodríguez.
