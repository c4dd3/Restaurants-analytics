# =============================================================================
# cargar_fact_reservaciones.py
# Job Spark: carga fact_reservaciones en Hive.
#
# Origen  : PostgreSQL del Proyecto 1 — tabla reservations
# Destino : Hive — restaurants_dw.fact_reservaciones
#
# Requiere que cargar_dimensiones.py haya corrido primero
# (necesita dim_tiempo, dim_usuario, dim_restaurante).
#
# Ejecutado por Airflow → SparkSubmitOperator (tarea: cargar_fact_reservaciones)
# =============================================================================

from __future__ import annotations

import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuración de conexión
# ---------------------------------------------------------------------------
PG_HOST     = os.getenv("POSTGRES_HOST",     "re2_postgres")
PG_PORT     = os.getenv("POSTGRES_PORT",     "5432")
PG_DB       = os.getenv("POSTGRES_DB",       "restaurants")
PG_USER     = os.getenv("POSTGRES_USER",     "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

JDBC_URL    = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS  = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

HIVE_DB = "restaurants_dw"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("cargar_fact_reservaciones")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_pg(spark: SparkSession, table: str):
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)


def main() -> None:
    spark = build_spark()

    # Limpiar tabla y directorio residual de ejecuciones previas fallidas
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.fact_reservaciones")
    table_dir = f"/opt/hive/data/warehouse/{HIVE_DB}.db/fact_reservaciones"
    if os.path.exists(table_dir):
        shutil.rmtree(table_dir, ignore_errors=True)
        print(f"[INFO] Directorio limpiado: {table_dir}")

    try:
        print("[INFO] Leyendo reservations de Postgres...")
        reservations = read_pg(spark, "reservations")

        print("[INFO] Leyendo dimensiones desde Hive...")
        dim_tiempo      = spark.table(f"{HIVE_DB}.dim_tiempo")
        dim_usuario     = spark.table(f"{HIVE_DB}.dim_usuario")
        dim_restaurante = spark.table(f"{HIVE_DB}.dim_restaurante")

        # Construir tiempo_key a partir de la fecha de la reservación
        # La tabla reservations usa columna "date" (DATE o TIMESTAMP)
        reservations = reservations.withColumn(
            "ts", F.col("date").cast("timestamp")
        ).withColumn(
            "tiempo_key",
            (F.year("ts") * 1000000 + F.month("ts") * 10000 +
             F.dayofmonth("ts") * 100 + F.hour("ts")).cast("bigint"),
        )

        # Surrogate keys
        reservations = (
            reservations
            .withColumn("usuario_key_calc",
                F.crc32(F.col("user_id").cast("string")).cast("bigint"))
            .withColumn("restaurante_key_calc",
                F.crc32(F.col("restaurant_id").cast("string")).cast("bigint"))
        )

        # Joins con dimensiones
        fact = (
            reservations
            .join(dim_tiempo.select("tiempo_key"),
                  reservations.tiempo_key == dim_tiempo.tiempo_key, "left")
            .join(dim_usuario.select("usuario_key"),
                  reservations.usuario_key_calc == dim_usuario.usuario_key, "left")
            .join(dim_restaurante.select("restaurante_key"),
                  reservations.restaurante_key_calc == dim_restaurante.restaurante_key, "left")
            .withColumn("reservacion_id", F.col("id").cast("string"))
            .withColumn("tamano_grupo",   F.col("party_size").cast("int"))
            .withColumn("estado",         F.col("status").cast("string"))
            .select(
                dim_tiempo.tiempo_key,
                dim_usuario.usuario_key,
                dim_restaurante.restaurante_key,
                F.col("reservacion_id"),
                F.col("tamano_grupo"),
                F.col("estado"),
            )
        )

        print("[INFO] Escribiendo fact_reservaciones en Hive...")
        (
            fact.write
            .mode("overwrite")
            .format("hive")
            .saveAsTable(f"{HIVE_DB}.fact_reservaciones")
        )
        print(f"[OK] fact_reservaciones: {fact.count()} filas cargadas")

    except Exception as e:
        print(f"[ERROR] Fallo en carga de fact_reservaciones: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
