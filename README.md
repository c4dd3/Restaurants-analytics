# Restaurants-analytics

Stack de analítica OLAP para el Proyecto 2 del curso Base de Datos 2 (TEC).  
Extiende el [Proyecto 1 (Restaurants-e2)](https://github.com/c4dd3/Restaurants-e2) con Data Warehouse, procesamiento con Spark, orquestación con Airflow, análisis de grafos con Neo4J y dashboards con Metabase.

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────┐
│                     Proyecto 1  —  re2_net                       │
│          Postgres          MongoDB          ElasticSearch         │
└────────────────────────────┬─────────────────────────────────────┘
                             │  red externa (RE2_NETWORK_NAME)
┌────────────────────────────▼─────────────────────────────────────┐
│                  Stack de Analítica  —  ra_net                    │
│                                                                   │
│   ┌──────────────────────┐        ┌───────────────────────────┐   │
│   │    Apache Airflow    │──────▶ │      Apache Spark         │   │
│   │    DAG: ETL pipeline │        │    Master  +  Worker      │   │
│   │    :8085             │        │    :8090 (UI)  /  :7077   │   │
│   └──────────────────────┘        └─────────────┬─────────────┘   │
│                                                 │                 │
│                  ┌──────────────────────────────▼─────────────┐   │
│                  │               Apache Hive                   │   │
│                  │   Metastore (:9083) + HiveServer2 (:10000)  │   │
│                  │   Data Warehouse — esquema estrella         │   │
│                  └──────────────┬──────────────────────────────┘   │
│                                 │                                 │
│   ┌─────────────────────────────▼──────┐   ┌──────────────────┐   │
│   │             Metabase               │   │      Neo4J       │   │
│   │   Dashboards de visualización      │   │  Grafos y rutas  │   │
│   │   :3000                            │   │  :7474  /  :7687 │   │
│   └────────────────────────────────────┘   └──────────────────┘   │
│                                                                   │
│   ┌───────────────────────────────────────────────────────────┐   │
│   │                      analytics-db                         │   │
│   │      Postgres:  hive_metastore  |  airflow  |  metabase   │   │
│   └───────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Componentes

| Servicio         | Imagen                    | Puerto(s)    | Función                                           |
|------------------|---------------------------|--------------|---------------------------------------------------|
| `analytics-db`   | postgres:16-alpine        | —            | Metastore de Hive, metadata de Airflow y Metabase |
| `hive-metastore` | Dockerfile.hive           | 9083         | Catálogo del Data Warehouse (Thrift)              |
| `hive-server`    | Dockerfile.hive           | 10000, 10002 | HiveServer2 / Beeline                             |
| `spark-master`   | bitnami/spark:3.5         | 8090, 7077   | Coordinador de jobs Spark                         |
| `spark-worker`   | bitnami/spark:3.5         | —            | Ejecutor de transformaciones                      |
| `airflow`        | Dockerfile.airflow        | 8085         | Orquestación del pipeline ETL                     |
| `neo4j`          | neo4j:5.20                | 7474, 7687   | Grafos de usuarios, productos y rutas             |
| `metabase`       | metabase/metabase:v0.50.0 | 3000         | Dashboards OLAP                                   |

---

## Requisitos previos

- Docker 24+ y Docker Compose v2
- Stack del **Proyecto 1** levantado (para que exista la red `re2_net`)
- Al menos 8 GB de RAM disponibles para Docker

---

## Setup

### 1. Clonar y configurar variables

```bash
git clone https://github.com/c4dd3/Restaurants-analytics
cd Restaurants-analytics
cp .env.example .env
```

Completar en `.env` las claves de Airflow:

```bash
# Fernet key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret key
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 2. Levantar el Proyecto 1 primero

```bash
# Desde el directorio de Restaurants-e2:
DB_ENGINE=postgres docker compose -f deployments/docker-compose.yml --profile postgres up -d
```

### 3. Detectar la red y levantar el stack

```bash
make setup   # detecta RE2_NETWORK_NAME y lo escribe en .env
make up      # construye imágenes y levanta todos los servicios
```

El primer arranque tarda ~3 minutos mientras Hive inicializa el metastore.

### 4. Verificar servicios

```bash
make ps
```

### 5. Acceder a las UIs

| Interfaz      | URL                    | Credenciales                        |
|---------------|------------------------|-------------------------------------|
| Airflow       | http://localhost:8085  | admin / admin                       |
| Spark Master  | http://localhost:8090  | —                                   |
| HiveServer2   | http://localhost:10002 | —                                   |
| Neo4J Browser | http://localhost:7474  | neo4j / (NEO4J_PASSWORD en .env)    |
| Metabase      | http://localhost:3000  | Configurar en primer acceso         |

### 6. Comandos útiles

```bash
make beeline      # shell interactivo de Hive
make neo4j-shell  # Cypher shell de Neo4J
make spark-shell  # Spark shell interactivo
make logs         # seguir logs de todos los servicios
make down         # bajar el stack
make down-v       # bajar y borrar volúmenes (limpieza total)
```

---

## Estructura del repositorio

```
Restaurants-analytics/
├── airflow/
│   ├── dags/                   # DAGs de Airflow (pipeline ETL)
│   ├── plugins/
│   │   ├── extractors/         # Operadores de extracción (Postgres, Mongo)
│   │   └── loaders/            # Operadores de carga (Hive, ElasticSearch)
│   └── requirements.txt        # Providers y dependencias de Airflow
├── dashboards/
│   └── metabase/
│       └── exports/            # Dashboards exportados (.json)
├── deployments/
│   ├── docker-compose.yml
│   ├── Dockerfile.hive         # Agrega driver JDBC de Postgres a Hive
│   ├── Dockerfile.airflow      # Agrega providers de Spark/Mongo/Postgres
│   ├── airflow/
│   │   └── entrypoint.sh       # Inicializa Airflow al arrancar el contenedor
│   └── postgres/
│       └── init-multiple-dbs.sh
├── docs/                       # Documentación técnica y diagramas
├── hive/
│   └── schema/                 # DDL del esquema estrella (HQL)
├── neo4j/
│   ├── import/                 # CSVs para carga inicial del grafo
│   └── queries/                # Consultas Cypher
├── spark/
│   ├── conf/
│   │   ├── hive-site.xml       # Apunta Spark al metastore de Hive
│   │   └── spark-defaults.conf # Configuración global de Spark
│   ├── jobs/                   # Scripts PySpark por análisis
│   └── utils/                  # Funciones compartidas entre jobs
├── .env.example
├── .gitignore
├── Makefile
└── README.md
```

---

## Flujo de datos

```
Postgres / MongoDB  (Proyecto 1)
          │
          │  Extracción  (Airflow)
          ▼
   Apache Spark  ──▶  Transformaciones (DataFrames / SparkSQL)
          │
          │  Carga
          ▼
   Apache Hive  ──▶  Data Warehouse (esquema estrella)
          │
          ├──▶  Metabase  ──▶  Dashboards
          │
          └──▶  ElasticSearch  (reindexado si cambia catálogo)

Postgres / MongoDB  (Proyecto 1)
          │
          │  Importación directa
          ▼
      Neo4J  ──▶  Grafos (co-compras, usuarios influyentes, rutas de reparto)
```

---

## Créditos

Proyecto universitario — Tecnológico de Costa Rica, Base de Datos 2.  
Profesor: Kenneth Obando Rodríguez.
