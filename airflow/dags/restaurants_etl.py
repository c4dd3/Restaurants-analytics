# =============================================================================
# restaurants_etl.py
# DAG principal del pipeline ETL de Restaurants-analytics.
#
# Flujo:
#   verificar_fuentes
#       └─→ cargar_dimensiones (Spark)
#               ├─→ cargar_fact_items_pedido (Spark)  ─┐
#               └─→ cargar_fact_reservaciones (Spark) ─┤
#                                                       └─→ verificar_cambios_productos
#                                                               ├─→ reindexar_elasticsearch
#                                                               └─→ sin_cambios (skip)
#                                                                       └─→ pipeline_completo
#
# Schedule: diario a las 2:00 AM
# =============================================================================

from __future__ import annotations

import hashlib
import os
import requests

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
#  Configuración por defecto de las tareas
# =============================================================================
DEFAULT_ARGS = {
    "owner": "persona-a",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Variables de entorno inyectadas por docker-compose
SPARK_MASTER    = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
PG_CONN_ID      = "postgres_proyecto1"       # Conexión definida en Airflow UI
SPARK_CONN_ID   = "spark_default"            # Conexión definida en Airflow UI
ES_REINDEX_URL  = "http://re2_nginx/search/reindex"   # Endpoint del Proyecto 1
HIVE_DB         = "restaurants_dw"

# Rutas de los jobs de Spark (montadas en /opt/spark-apps)
JOBS_PATH = "/opt/spark-apps/jobs"

# Configuración compartida para todos los SparkSubmitOperator
SPARK_CONF = {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "/opt/hive/data/warehouse",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.jars": "/opt/airflow/jars/postgresql-42.7.3.jar",
    # El driver corre dentro del contenedor ra_airflow.
    # Hostname con guión (no underscore) — Java URI (RFC 2396) rechaza underscores
    # en hostnames, lo que causa "Invalid Spark URL" en el executor.
    "spark.driver.host": "ra-airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    # Commit v2: cada executor hace commit de su tarea directamente al destino
    # final sin depender del driver para el rename del staging.
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
}


# =============================================================================
#  DAG
# =============================================================================
@dag(
    dag_id="restaurants_etl",
    description="Pipeline ETL diario: Proyecto 1 → Spark → Hive DW → ElasticSearch",
    schedule="0 2 * * *",          # Todos los días a las 2:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "hive", "spark", "proyecto2"],
)
def restaurants_etl():

    # -------------------------------------------------------------------------
    #  TAREA 1 — Verificar que las fuentes del Proyecto 1 están accesibles
    # -------------------------------------------------------------------------
    @task(task_id="verificar_fuentes")
    def verificar_fuentes():
        """
        Comprueba que Postgres del Proyecto 1 responde antes de arrancar el pipeline.
        Lanza una excepción si no hay conexión, lo que detiene el DAG limpiamente.
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders;")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✓ Fuente Postgres accesible — {count} órdenes disponibles")
        return count

    # -------------------------------------------------------------------------
    #  TAREA 2 — Cargar dimensiones (Spark)
    #  Debe correr antes que las fact tables porque estas hacen join con las dims.
    # -------------------------------------------------------------------------
    cargar_dimensiones = SparkSubmitOperator(
        task_id="cargar_dimensiones",
        application=f"{JOBS_PATH}/cargar_dimensiones.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        name="etl_cargar_dimensiones",
        verbose=False,
    )

    # -------------------------------------------------------------------------
    #  TAREA 3a — Cargar fact_items_pedido (Spark)
    # -------------------------------------------------------------------------
    cargar_fact_items = SparkSubmitOperator(
        task_id="cargar_fact_items_pedido",
        application=f"{JOBS_PATH}/cargar_fact_items_pedido.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        name="etl_fact_items_pedido",
        verbose=False,
    )

    # -------------------------------------------------------------------------
    #  TAREA 3b — Cargar fact_reservaciones (Spark) — paralela con 3a
    # -------------------------------------------------------------------------
    cargar_fact_reservaciones = SparkSubmitOperator(
        task_id="cargar_fact_reservaciones",
        application=f"{JOBS_PATH}/cargar_fact_reservaciones.py",
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        name="etl_fact_reservaciones",
        verbose=False,
    )

    # -------------------------------------------------------------------------
    #  TAREA 4 — Verificar si el catálogo de productos cambió
    #  Compara un hash del catálogo actual contra el de la última ejecución
    #  (guardado en la variable de Airflow "products_catalog_hash").
    # -------------------------------------------------------------------------
    def _verificar_cambios_productos(**context):
        from airflow.models import Variable

        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Obtener todos los productos como string determinístico
        cursor.execute("""
            SELECT id, name, category, price, available
            FROM products
            ORDER BY id;
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # Calcular hash del catálogo actual
        catalog_str = str(rows).encode("utf-8")
        hash_actual = hashlib.md5(catalog_str).hexdigest()

        # Comparar con el hash de la ejecución anterior
        hash_anterior = Variable.get("products_catalog_hash", default_var=None)
        print(f"Hash anterior: {hash_anterior}")
        print(f"Hash actual:   {hash_actual}")

        if hash_actual != hash_anterior:
            # Guardar el nuevo hash para la próxima ejecución
            Variable.set("products_catalog_hash", hash_actual)
            print("⚠ Catálogo de productos cambió — se reindexará ElasticSearch")
            return "reindexar_elasticsearch"
        else:
            print("✓ Catálogo sin cambios — se omite el reindexado")
            return "sin_cambios"

    verificar_cambios = BranchPythonOperator(
        task_id="verificar_cambios_productos",
        python_callable=_verificar_cambios_productos,
    )

    # -------------------------------------------------------------------------
    #  TAREA 5a — Reindexar ElasticSearch (si el catálogo cambió)
    #  Llama al endpoint POST /search/reindex del Proyecto 1.
    # -------------------------------------------------------------------------
    @task(task_id="reindexar_elasticsearch")
    def reindexar_elasticsearch():
        """
        Dispara el reindexado del catálogo de productos en ElasticSearch
        usando el endpoint del servicio search del Proyecto 1.
        Requiere que el token de admin esté guardado en la variable
        Airflow 'api_admin_token'.
        """
        from airflow.models import Variable
        token = Variable.get("api_admin_token", default_var="")

        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(ES_REINDEX_URL, headers=headers, timeout=30)
        response.raise_for_status()
        print(f"✓ Reindexado completado — status {response.status_code}")
        return response.json()

    # -------------------------------------------------------------------------
    #  TAREA 5b — Skip (sin cambios en el catálogo)
    # -------------------------------------------------------------------------
    sin_cambios = EmptyOperator(task_id="sin_cambios")

    # -------------------------------------------------------------------------
    #  TAREA 6 — Fin del pipeline
    #  trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS: corre aunque una rama del
    #  BranchPythonOperator haya sido skipped.
    # -------------------------------------------------------------------------
    pipeline_completo = EmptyOperator(
        task_id="pipeline_completo",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    #  Definición del flujo (dependencias entre tareas)
    # =========================================================================
    inicio = verificar_fuentes()

    # Verificar → dimensiones → facts en paralelo
    inicio >> cargar_dimensiones
    cargar_dimensiones >> [cargar_fact_items, cargar_fact_reservaciones]

    # Ambas facts deben completarse antes de verificar cambios
    [cargar_fact_items, cargar_fact_reservaciones] >> verificar_cambios

    # Branch: reindexar o skip → siempre termina en pipeline_completo
    verificar_cambios >> [reindexar_elasticsearch(), sin_cambios]
    [reindexar_elasticsearch(), sin_cambios] >> pipeline_completo


# Instanciar el DAG
dag_instance = restaurants_etl()
