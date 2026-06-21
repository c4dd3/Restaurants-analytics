# =============================================================================
# cargar_fact_items_pedido.py
# Job Spark: carga fact_items_pedido en Hive.
#
# Origen  : PostgreSQL del Proyecto 1 — tablas orders + order_items
# Destino : Hive — restaurants_dw.fact_items_pedido
#
# Requiere que cargar_dimensiones.py haya corrido primero
# (necesita dim_tiempo, dim_usuario, dim_restaurante, dim_producto).
#
# Ejecutado por Airflow → SparkSubmitOperator (tarea: cargar_fact_items_pedido)
# =============================================================================

from __future__ import annotations

import os
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
        .appName("cargar_fact_items_pedido")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_pg(spark: SparkSession, table: str):
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)


def main() -> None:
    spark = build_spark()

    try:
        print("[INFO] Leyendo orders y order_items de Postgres...")
        orders      = read_pg(spark, "orders")
        order_items = read_pg(spark, "order_items")

        # Unir orders + order_items para tener todos los campos
        items = (
            order_items
            .join(orders, order_items.order_id == orders.id, "inner")
            .select(
                order_items.id.alias("item_id"),
                order_items.order_id,
                order_items.product_id,
                order_items.quantity,
                order_items.price.alias("precio_unitario"),
                orders.user_id,
                orders.restaurant_id,
                orders.status.alias("estado_pedido"),
                orders.pickup.alias("es_para_llevar"),
                orders.created_at,
            )
        )

        print("[INFO] Leyendo dimensiones desde Hive...")
        dim_tiempo      = spark.table(f"{HIVE_DB}.dim_tiempo")
        dim_usuario     = spark.table(f"{HIVE_DB}.dim_usuario")
        dim_restaurante = spark.table(f"{HIVE_DB}.dim_restaurante")
        dim_producto    = spark.table(f"{HIVE_DB}.dim_producto")

        # Construir tiempo_key a partir de created_at (mismo cálculo que en dim_tiempo)
        items = items.withColumn(
            "tiempo_key",
            (F.year("created_at") * 1000000 + F.month("created_at") * 10000 +
             F.dayofmonth("created_at") * 100 + F.hour("created_at")).cast("bigint"),
        )

        # Surrogate keys de usuario, restaurante y producto usando crc32
        items = (
            items
            .withColumn("usuario_key_calc",     F.crc32(F.col("user_id").cast("string")).cast("bigint"))
            .withColumn("restaurante_key_calc",  F.crc32(F.col("restaurant_id").cast("string")).cast("bigint"))
            .withColumn("producto_key_calc",     F.crc32(F.col("product_id").cast("string")).cast("bigint"))
        )

        # Joins con dimensiones para obtener las llaves surrogate oficiales
        fact = (
            items
            .join(dim_tiempo.select("tiempo_key"),
                  items.tiempo_key == dim_tiempo.tiempo_key, "left")
            .join(dim_usuario.select("usuario_key"),
                  items.usuario_key_calc == dim_usuario.usuario_key, "left")
            .join(dim_restaurante.select("restaurante_key"),
                  items.restaurante_key_calc == dim_restaurante.restaurante_key, "left")
            .join(dim_producto.select("producto_key"),
                  items.producto_key_calc == dim_producto.producto_key, "left")
            .withColumn("precio_unitario", F.col("precio_unitario").cast("decimal(10,2)"))
            .withColumn("monto_total",
                (F.col("cantidad") * F.col("precio_unitario")).cast("decimal(10,2)"))
            .withColumn("cantidad",        F.col("quantity").cast("int"))
            .withColumn("pedido_id",       F.col("order_id").cast("string"))
            .withColumn("item_id",         F.col("item_id").cast("string"))
            .withColumn("es_para_llevar",  F.col("es_para_llevar").cast("boolean"))
            .select(
                dim_tiempo.tiempo_key,
                dim_usuario.usuario_key,
                dim_restaurante.restaurante_key,
                dim_producto.producto_key,
                F.col("pedido_id"),
                F.col("item_id"),
                F.col("cantidad"),
                F.col("precio_unitario"),
                F.col("monto_total"),
                F.col("estado_pedido"),
                F.col("es_para_llevar"),
            )
        )

        print("[INFO] Escribiendo fact_items_pedido en Hive...")
        (
            fact.write
            .mode("overwrite")
            .format("hive")
            .saveAsTable(f"{HIVE_DB}.fact_items_pedido")
        )
        print(f"[OK] fact_items_pedido: {fact.count()} filas cargadas")

    except Exception as e:
        print(f"[ERROR] Fallo en carga de fact_items_pedido: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
