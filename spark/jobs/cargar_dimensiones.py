# =============================================================================
# cargar_dimensiones.py
# Job Spark: carga las 4 tablas de dimensión del esquema estrella en Hive.
#
# Origen  : PostgreSQL del Proyecto 1 (re2_postgres)
# Destino : Hive — restaurants_dw.dim_tiempo
#                  restaurants_dw.dim_usuario
#                  restaurants_dw.dim_restaurante
#                  restaurants_dw.dim_producto
#
# Ejecutado por Airflow → SparkSubmitOperator (tarea: cargar_dimensiones)
# =============================================================================

from __future__ import annotations

import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuración de conexión (inyectada por docker-compose / Airflow)
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

# Expresiones nativas Spark para nombres de mes y día (evitan Python UDFs
# y el consiguiente PYTHON_VERSION_MISMATCH entre driver 3.12 y worker 3.8).
def _nombre_mes_col(mes_col):
    return (
        F.when(mes_col == 1,  "Enero")
        .when(mes_col == 2,  "Febrero")
        .when(mes_col == 3,  "Marzo")
        .when(mes_col == 4,  "Abril")
        .when(mes_col == 5,  "Mayo")
        .when(mes_col == 6,  "Junio")
        .when(mes_col == 7,  "Julio")
        .when(mes_col == 8,  "Agosto")
        .when(mes_col == 9,  "Septiembre")
        .when(mes_col == 10, "Octubre")
        .when(mes_col == 11, "Noviembre")
        .when(mes_col == 12, "Diciembre")
        .otherwise("")
    )

def _nombre_dia_col(dia_col):
    return (
        F.when(dia_col == 1, "Lunes")
        .when(dia_col == 2, "Martes")
        .when(dia_col == 3, "Miércoles")
        .when(dia_col == 4, "Jueves")
        .when(dia_col == 5, "Viernes")
        .when(dia_col == 6, "Sábado")
        .when(dia_col == 7, "Domingo")
        .otherwise("")
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("cargar_dimensiones")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_pg(spark: SparkSession, table: str):
    """Lee una tabla de PostgreSQL via JDBC."""
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)


# ---------------------------------------------------------------------------
# dim_tiempo
# Se construye a partir de todos los timestamps de orders y reservations.
# Grano: una fila por hora única que aparezca en los datos.
# ---------------------------------------------------------------------------
def cargar_dim_tiempo(spark: SparkSession) -> None:
    print("[INFO] Cargando dim_tiempo...")

    orders       = read_pg(spark, "orders")
    reservations = read_pg(spark, "reservations")

    # Recopilar todos los timestamps relevantes
    ts_orders = orders.select(F.col("created_at").alias("ts"))
    ts_reserv = reservations.select(F.col("date").cast("timestamp").alias("ts"))

    timestamps = ts_orders.union(ts_reserv).dropna().distinct()

    dim = (
        timestamps
        .withColumn("fecha",       F.to_date("ts"))
        .withColumn("anio",        F.year("ts").cast("int"))
        .withColumn("trimestre",   F.quarter("ts").cast("int"))
        .withColumn("mes",         F.month("ts").cast("int"))
        .withColumn("semana",      F.weekofyear("ts").cast("int"))
        .withColumn("dia",         F.dayofmonth("ts").cast("int"))
        .withColumn("dia_semana",  F.dayofweek("ts").cast("int"))  # 1=Dom … 7=Sáb en Spark
        .withColumn("hora",        F.hour("ts").cast("int"))
        # Surrogate key: YYYYMMDDhh
        .withColumn("tiempo_key",
            (F.year("ts") * 1000000 + F.month("ts") * 10000 +
             F.dayofmonth("ts") * 100 + F.hour("ts")).cast("bigint"))
        .withColumn("nombre_mes",  _nombre_mes_col(F.col("mes")))
        .withColumn("nombre_dia",  _nombre_dia_col(F.col("dia_semana")))
        .withColumn("es_fin_semana",
            F.col("dia_semana").isin([1, 7]))   # 1=Dom, 7=Sáb en Spark
        .select("tiempo_key", "fecha", "anio", "trimestre", "mes",
                "nombre_mes", "semana", "dia", "dia_semana", "nombre_dia",
                "hora", "es_fin_semana")
        .dropDuplicates(["tiempo_key"])
        .orderBy("tiempo_key")
    )

    (
        dim.write
        .mode("overwrite")
        .format("hive")
        .saveAsTable(f"{HIVE_DB}.dim_tiempo")
    )
    print(f"[OK] dim_tiempo: {dim.count()} filas cargadas")


# ---------------------------------------------------------------------------
# dim_usuario
# ---------------------------------------------------------------------------
def cargar_dim_usuario(spark: SparkSession) -> None:
    print("[INFO] Cargando dim_usuario...")

    users = read_pg(spark, "users")

    dim = (
        users
        .withColumn("usuario_key",    F.crc32(F.col("id").cast("string")).cast("bigint"))
        .withColumn("usuario_id",     F.col("id").cast("string"))
        .withColumn("nombre",         F.col("name"))
        .withColumn("rol",            F.col("role"))
        .withColumn("fecha_registro", F.to_date(F.col("created_at")))
        .select("usuario_key", "usuario_id", "nombre", "rol", "fecha_registro")
        .dropDuplicates(["usuario_key"])
    )

    (
        dim.write
        .mode("overwrite")
        .format("hive")
        .saveAsTable(f"{HIVE_DB}.dim_usuario")
    )
    print(f"[OK] dim_usuario: {dim.count()} filas cargadas")


# ---------------------------------------------------------------------------
# dim_restaurante
# ---------------------------------------------------------------------------
def cargar_dim_restaurante(spark: SparkSession) -> None:
    print("[INFO] Cargando dim_restaurante...")

    restaurants = read_pg(spark, "restaurants")

    dim = (
        restaurants
        .withColumn("restaurante_key", F.crc32(F.col("id").cast("string")).cast("bigint"))
        .withColumn("restaurante_id",  F.col("id").cast("string"))
        .withColumn("nombre",          F.col("name"))
        .withColumn("direccion",       F.col("address"))
        .withColumn("capacidad",       F.col("capacity").cast("int"))
        .select("restaurante_key", "restaurante_id", "nombre", "direccion", "capacidad")
        .dropDuplicates(["restaurante_key"])
    )

    (
        dim.write
        .mode("overwrite")
        .format("hive")
        .saveAsTable(f"{HIVE_DB}.dim_restaurante")
    )
    print(f"[OK] dim_restaurante: {dim.count()} filas cargadas")


# ---------------------------------------------------------------------------
# dim_producto
# ---------------------------------------------------------------------------
def cargar_dim_producto(spark: SparkSession) -> None:
    print("[INFO] Cargando dim_producto...")

    products = read_pg(spark, "products")

    dim = (
        products
        .withColumn("producto_key",   F.crc32(F.col("id").cast("string")).cast("bigint"))
        .withColumn("producto_id",    F.col("id").cast("string"))
        .withColumn("nombre",         F.col("name"))
        .withColumn("categoria",      F.col("category"))
        .withColumn("precio_actual",  F.col("price").cast("decimal(10,2)"))
        .withColumn("disponible",     F.col("available").cast("boolean"))
        .withColumn("restaurante_id", F.col("restaurant_id").cast("string"))
        .select("producto_key", "producto_id", "nombre", "categoria",
                "precio_actual", "disponible", "restaurante_id")
        .dropDuplicates(["producto_key"])
    )

    (
        dim.write
        .mode("overwrite")
        .format("hive")
        .saveAsTable(f"{HIVE_DB}.dim_producto")
    )
    print(f"[OK] dim_producto: {dim.count()} filas cargadas")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _drop_table_and_dir(spark: SparkSession, table: str) -> None:
    """Elimina la tabla del metastore y cualquier directorio residual en el warehouse."""
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    _, tbl = table.split(".")
    table_dir = f"/opt/hive/data/warehouse/{HIVE_DB}.db/{tbl}"
    if os.path.exists(table_dir):
        shutil.rmtree(table_dir, ignore_errors=True)
        print(f"[INFO] Directorio limpiado: {table_dir}")


def main() -> None:
    spark = build_spark()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")

    # Limpiar tablas y directorios residuales de ejecuciones previas fallidas
    for tbl in ["dim_tiempo", "dim_usuario", "dim_restaurante", "dim_producto"]:
        _drop_table_and_dir(spark, f"{HIVE_DB}.{tbl}")

    try:
        cargar_dim_tiempo(spark)
        cargar_dim_usuario(spark)
        cargar_dim_restaurante(spark)
        cargar_dim_producto(spark)
        print("[OK] Todas las dimensiones cargadas exitosamente.")
    except Exception as e:
        print(f"[ERROR] Fallo en carga de dimensiones: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
