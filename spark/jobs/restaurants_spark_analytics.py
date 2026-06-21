"""
Job PySpark para el Proyecto 2 de Restaurants Analytics.

Cubre la parte de Procesamiento con Apache Spark:
- DataFrames y SparkSQL.
- Tendencias de consumo.
- Horarios pico.
- Crecimiento mensual.
- Exportación de archivos base para Neo4J y rutas de entrega.

Modo recomendado para pruebas rápidas:
  spark-submit --master spark://spark-master:7077 /opt/spark-apps/jobs/restaurants_spark_analytics.py --source sample
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


DEFAULT_CENTER_LAT = 9.9326
DEFAULT_CENTER_LON = -84.0796


@dataclass
class AppConfig:
    source: str
    output_base: str
    neo4j_output: str
    couriers: int
    postgres_url: str
    postgres_user: str
    postgres_password: str
    mongo_uri: str
    center_lat: float
    center_lon: float
    write_hive: bool


def parse_args() -> AppConfig:
    parser = argparse.ArgumentParser(description="Restaurants analytics Spark job")
    parser.add_argument("--source", choices=["sample", "mongo", "postgres"], default=os.getenv("ANALYTICS_SOURCE", "sample"))
    parser.add_argument("--output-base", default=os.getenv("ANALYTICS_OUTPUT_BASE", "/opt/spark-apps/output"))
    parser.add_argument("--neo4j-output", default=os.getenv("NEO4J_IMPORT_DIR", "/opt/neo4j-import"))
    parser.add_argument("--couriers", type=int, default=int(os.getenv("ROUTE_COURIERS", "2")))
    parser.add_argument("--postgres-url", default=os.getenv("PG_JDBC_URL", "jdbc:postgresql://re2_postgres:5432/restaurants"))
    parser.add_argument("--postgres-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--postgres-password", default=os.getenv("PG_PASSWORD", "postgres"))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://re2_mongos:27017/restaurants"))
    parser.add_argument("--center-lat", type=float, default=float(os.getenv("ROUTE_CENTER_LAT", str(DEFAULT_CENTER_LAT))))
    parser.add_argument("--center-lon", type=float, default=float(os.getenv("ROUTE_CENTER_LON", str(DEFAULT_CENTER_LON))))
    parser.add_argument("--no-hive", action="store_true", help="No intenta crear tablas Hive")
    args = parser.parse_args()

    return AppConfig(
        source=args.source,
        output_base=args.output_base,
        neo4j_output=args.neo4j_output,
        couriers=max(args.couriers, 1),
        postgres_url=args.postgres_url,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
        mongo_uri=args.mongo_uri,
        center_lat=args.center_lat,
        center_lon=args.center_lon,
        write_hive=not args.no_hive,
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("restaurants-spark-analytics")
        .enableHiveSupport()
        .getOrCreate()
    )


def sample_data(spark: SparkSession) -> Dict[str, DataFrame]:
    users_schema = T.StructType([
        T.StructField("user_id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("zone", T.StringType(), True),
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("referred_by", T.StringType(), True),
    ])
    products_schema = T.StructType([
        T.StructField("product_id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
    ])
    orders_schema = T.StructType([
        T.StructField("order_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("restaurant_id", T.StringType(), True),
        T.StructField("created_at", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("delivery_latitude", T.DoubleType(), True),
        T.StructField("delivery_longitude", T.DoubleType(), True),
        T.StructField("zone", T.StringType(), True),
    ])
    order_items_schema = T.StructType([
        T.StructField("order_id", T.StringType(), False),
        T.StructField("product_id", T.StringType(), False),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
    ])
    reservations_schema = T.StructType([
        T.StructField("reservation_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("restaurant_id", T.StringType(), True),
        T.StructField("reservation_time", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("party_size", T.IntegerType(), True),
    ])

    users = spark.createDataFrame([
        ("u1", "Ana", "San Pedro", 9.9331, -84.0502, None),
        ("u2", "Luis", "Curridabat", 9.9149, -84.0348, "u1"),
        ("u3", "Sofía", "Escazú", 9.9182, -84.1399, "u1"),
        ("u4", "Marco", "Heredia", 9.9980, -84.1198, "u2"),
        ("u5", "Valeria", "Cartago", 9.8644, -83.9194, None),
    ], users_schema)

    products = spark.createDataFrame([
        ("p1", "Pizza margarita", "pizzas", 4500.0),
        ("p2", "Pizza pepperoni", "pizzas", 5200.0),
        ("p3", "Hamburguesa clásica", "hamburguesas", 3900.0),
        ("p4", "Papas medianas", "acompanamientos", 1500.0),
        ("p5", "Limonada natural", "bebidas", 1800.0),
        ("p6", "Brownie", "postres", 1600.0),
    ], products_schema)

    orders = spark.createDataFrame([
        ("o1", "u1", "r1", "2026-04-01 12:15:00", "completed", 9.9331, -84.0502, "San Pedro"),
        ("o2", "u2", "r1", "2026-04-01 12:45:00", "completed", 9.9149, -84.0348, "Curridabat"),
        ("o3", "u3", "r2", "2026-04-03 19:30:00", "completed", 9.9182, -84.1399, "Escazú"),
        ("o4", "u4", "r2", "2026-05-05 20:10:00", "cancelled", 9.9980, -84.1198, "Heredia"),
        ("o5", "u5", "r3", "2026-05-12 13:20:00", "completed", 9.8644, -83.9194, "Cartago"),
        ("o6", "u1", "r1", "2026-05-20 19:05:00", "completed", 9.9331, -84.0502, "San Pedro"),
        ("o7", "u2", "r1", "2026-06-02 12:05:00", "completed", 9.9149, -84.0348, "Curridabat"),
        ("o8", "u3", "r2", "2026-06-03 21:00:00", "completed", 9.9182, -84.1399, "Escazú"),
    ], orders_schema)

    order_items = spark.createDataFrame([
        ("o1", "p1", 1, 4500.0), ("o1", "p5", 2, 1800.0),
        ("o2", "p2", 1, 5200.0), ("o2", "p4", 1, 1500.0), ("o2", "p5", 1, 1800.0),
        ("o3", "p3", 2, 3900.0), ("o3", "p4", 2, 1500.0),
        ("o4", "p1", 1, 4500.0), ("o4", "p6", 1, 1600.0),
        ("o5", "p3", 1, 3900.0), ("o5", "p5", 1, 1800.0),
        ("o6", "p1", 2, 4500.0), ("o6", "p6", 2, 1600.0),
        ("o7", "p2", 1, 5200.0), ("o7", "p5", 2, 1800.0),
        ("o8", "p3", 1, 3900.0), ("o8", "p4", 1, 1500.0), ("o8", "p6", 1, 1600.0),
    ], order_items_schema)

    reservations = spark.createDataFrame([
        ("res1", "u1", "r1", "2026-04-01 18:00:00", "confirmed", 2),
        ("res2", "u2", "r1", "2026-04-02 19:30:00", "cancelled", 4),
        ("res3", "u3", "r2", "2026-05-10 20:00:00", "confirmed", 3),
        ("res4", "u4", "r2", "2026-05-15 12:30:00", "confirmed", 5),
        ("res5", "u5", "r3", "2026-06-05 13:00:00", "completed", 2),
    ], reservations_schema)

    return {
        "users": users,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "reservations": reservations,
    }


def read_postgres_table(spark: SparkSession, cfg: AppConfig, table: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", cfg.postgres_url)
        .option("dbtable", table)
        .option("user", cfg.postgres_user)
        .option("password", cfg.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def read_mongo_collection(spark: SparkSession, cfg: AppConfig, collection: str) -> DataFrame:
    return (
        spark.read.format("mongodb")
        .option("spark.mongodb.read.connection.uri", cfg.mongo_uri)
        .option("connection.uri", cfg.mongo_uri)
        .option("uri", cfg.mongo_uri)
        .option("database", "restaurants")
        .option("collection", collection)
        .load()
    )


def read_source(spark: SparkSession, cfg: AppConfig) -> Dict[str, DataFrame]:
    if cfg.source == "sample":
        print("[INFO] Usando datos de ejemplo para validar el job sin depender del Proyecto 1")
        return sample_data(spark)

    try:
        if cfg.source == "postgres":
            print("[INFO] Leyendo tablas desde PostgreSQL")
            return {
                "users": read_postgres_table(spark, cfg, "users"),
                "products": read_postgres_table(spark, cfg, "products"),
                "orders": read_postgres_table(spark, cfg, "orders"),
                "order_items": read_postgres_table(spark, cfg, "order_items"),
                "reservations": read_postgres_table(spark, cfg, "reservations"),
            }

        print("[INFO] Leyendo colecciones desde MongoDB")
        return {
            "users": read_mongo_collection(spark, cfg, "users"),
            "products": read_mongo_collection(spark, cfg, "products"),
            "orders": read_mongo_collection(spark, cfg, "orders"),
            "order_items": read_mongo_collection(spark, cfg, "order_items"),
            "reservations": read_mongo_collection(spark, cfg, "reservations"),
        }
    except Exception as exc:
        print(f"[WARN] No se pudo leer {cfg.source}: {exc}")
        print("[WARN] Se continúa con datos sample para que el pipeline pueda probarse")
        return sample_data(spark)


def column_or_null(df: DataFrame, name: str, data_type: str = "string"):
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(data_type)


def normalize_sources(raw: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    users_raw = raw["users"]
    products_raw = raw["products"]
    orders_raw = raw["orders"]
    items_raw = raw["order_items"]
    reservations_raw = raw["reservations"]

    users = users_raw.select(
        column_or_null(users_raw, "user_id").cast("string").alias("user_id"),
        column_or_null(users_raw, "name").cast("string").alias("name"),
        F.coalesce(column_or_null(users_raw, "zone"), column_or_null(users_raw, "city"), F.lit("Sin zona")).alias("zone"),
        column_or_null(users_raw, "latitude", "double").cast("double").alias("latitude"),
        column_or_null(users_raw, "longitude", "double").cast("double").alias("longitude"),
        column_or_null(users_raw, "referred_by").cast("string").alias("referred_by"),
    ).where(F.col("user_id").isNotNull())

    products = products_raw.select(
        F.coalesce(column_or_null(products_raw, "product_id"), column_or_null(products_raw, "id"), column_or_null(products_raw, "_id")).cast("string").alias("product_id"),
        column_or_null(products_raw, "name").cast("string").alias("name"),
        F.coalesce(column_or_null(products_raw, "category"), F.lit("sin_categoria")).cast("string").alias("category"),
        F.coalesce(column_or_null(products_raw, "price", "double").cast("double"), F.lit(0.0)).alias("price"),
    ).where(F.col("product_id").isNotNull())

    orders = orders_raw.select(
        F.coalesce(column_or_null(orders_raw, "order_id"), column_or_null(orders_raw, "id"), column_or_null(orders_raw, "_id")).cast("string").alias("order_id"),
        column_or_null(orders_raw, "user_id").cast("string").alias("user_id"),
        column_or_null(orders_raw, "restaurant_id").cast("string").alias("restaurant_id"),
        F.to_timestamp(column_or_null(orders_raw, "created_at")).alias("created_at"),
        F.coalesce(column_or_null(orders_raw, "status"), F.lit("unknown")).cast("string").alias("status"),
        F.coalesce(column_or_null(orders_raw, "delivery_latitude", "double"), column_or_null(orders_raw, "latitude", "double")).cast("double").alias("delivery_latitude"),
        F.coalesce(column_or_null(orders_raw, "delivery_longitude", "double"), column_or_null(orders_raw, "longitude", "double")).cast("double").alias("delivery_longitude"),
        F.coalesce(column_or_null(orders_raw, "zone"), F.lit("Sin zona")).alias("zone"),
    ).where(F.col("order_id").isNotNull())

    items = items_raw.select(
        column_or_null(items_raw, "order_id").cast("string").alias("order_id"),
        F.coalesce(column_or_null(items_raw, "product_id"), column_or_null(items_raw, "id")).cast("string").alias("product_id"),
        F.coalesce(column_or_null(items_raw, "quantity", "int").cast("int"), F.lit(1)).alias("quantity"),
        F.coalesce(column_or_null(items_raw, "unit_price", "double").cast("double"), column_or_null(items_raw, "price", "double").cast("double"), F.lit(0.0)).alias("unit_price"),
    ).where(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())

    reservations = reservations_raw.select(
        F.coalesce(column_or_null(reservations_raw, "reservation_id"), column_or_null(reservations_raw, "id"), column_or_null(reservations_raw, "_id")).cast("string").alias("reservation_id"),
        column_or_null(reservations_raw, "user_id").cast("string").alias("user_id"),
        column_or_null(reservations_raw, "restaurant_id").cast("string").alias("restaurant_id"),
        F.to_timestamp(F.coalesce(column_or_null(reservations_raw, "reservation_time"), column_or_null(reservations_raw, "created_at"))).alias("reservation_time"),
        F.coalesce(column_or_null(reservations_raw, "status"), F.lit("unknown")).cast("string").alias("status"),
        F.coalesce(column_or_null(reservations_raw, "party_size", "int").cast("int"), F.lit(1)).alias("party_size"),
    ).where(F.col("reservation_id").isNotNull())

    return {
        "users": users,
        "products": products,
        "orders": orders,
        "order_items": items,
        "reservations": reservations,
    }


def build_fact_sales(data: Dict[str, DataFrame]) -> DataFrame:
    orders = data["orders"]
    items = data["order_items"]
    products = data["products"]

    fact_sales = (
        orders.alias("o")
        .join(items.alias("i"), "order_id", "inner")
        .join(products.alias("p"), "product_id", "left")
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
        .withColumn("order_date", F.to_date("created_at"))
        .withColumn("order_hour", F.hour("created_at"))
        .withColumn("order_month", F.date_format("created_at", "yyyy-MM"))
        .select(
            "order_id",
            "user_id",
            "restaurant_id",
            "product_id",
            F.col("p.name").alias("product_name"),
            "category",
            "quantity",
            "unit_price",
            "line_total",
            "created_at",
            "order_date",
            "order_hour",
            "order_month",
            "status",
            "zone",
            "delivery_latitude",
            "delivery_longitude",
        )
    )
    return fact_sales


def run_analysis(spark: SparkSession, data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    fact_sales = build_fact_sales(data)
    fact_sales.createOrReplaceTempView("fact_sales")
    data["reservations"].createOrReplaceTempView("reservations")

    consumption_trends = spark.sql(
        """
        SELECT
            order_date,
            category,
            product_id,
            product_name,
            SUM(quantity) AS units_sold,
            ROUND(SUM(line_total), 2) AS revenue,
            COUNT(DISTINCT order_id) AS orders_count
        FROM fact_sales
        WHERE status = 'completed'
        GROUP BY order_date, category, product_id, product_name
        ORDER BY order_date, revenue DESC
        """
    )

    peak_hours = spark.sql(
        """
        SELECT
            order_hour,
            COUNT(DISTINCT order_id) AS orders_count,
            ROUND(SUM(line_total), 2) AS revenue,
            SUM(quantity) AS units_sold
        FROM fact_sales
        WHERE status = 'completed' AND order_hour IS NOT NULL
        GROUP BY order_hour
        ORDER BY orders_count DESC, revenue DESC
        """
    )

    monthly_base = spark.sql(
        """
        SELECT
            order_month,
            COUNT(DISTINCT order_id) AS orders_count,
            ROUND(SUM(line_total), 2) AS revenue
        FROM fact_sales
        WHERE status = 'completed' AND order_month IS NOT NULL
        GROUP BY order_month
        """
    )
    window = Window.orderBy("order_month")
    monthly_growth = (
        monthly_base
        .withColumn("previous_month_revenue", F.lag("revenue").over(window))
        .withColumn(
            "growth_pct",
            F.when(F.col("previous_month_revenue").isNull(), F.lit(None).cast("double"))
            .when(F.col("previous_month_revenue") == 0, F.lit(None).cast("double"))
            .otherwise(F.round(((F.col("revenue") - F.col("previous_month_revenue")) / F.col("previous_month_revenue")) * 100, 2)),
        )
        .orderBy("order_month")
    )

    reservations_summary = spark.sql(
        """
        SELECT
            DATE_FORMAT(reservation_time, 'yyyy-MM') AS reservation_month,
            status,
            COUNT(*) AS reservations_count,
            SUM(party_size) AS total_people
        FROM reservations
        GROUP BY DATE_FORMAT(reservation_time, 'yyyy-MM'), status
        ORDER BY reservation_month, status
        """
    )

    co_purchases = (
        data["order_items"].alias("a")
        .join(data["order_items"].alias("b"), "order_id")
        .where(F.col("a.product_id") < F.col("b.product_id"))
        .groupBy(F.col("a.product_id").alias("product_a_id"), F.col("b.product_id").alias("product_b_id"))
        .agg(F.countDistinct("order_id").alias("times_bought_together"))
        .orderBy(F.desc("times_bought_together"), "product_a_id", "product_b_id")
    )

    return {
        "fact_sales": fact_sales,
        "consumption_trends": consumption_trends,
        "peak_hours": peak_hours,
        "monthly_growth": monthly_growth,
        "reservations_summary": reservations_summary,
        "co_purchases": co_purchases,
    }


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def nearest_neighbor_route(points: List[dict], start_lat: float, start_lon: float) -> List[Tuple[int, dict, float]]:
    pending = points[:]
    route = []
    current_lat = start_lat
    current_lon = start_lon
    stop = 1

    while pending:
        next_index = min(
            range(len(pending)),
            key=lambda idx: haversine_km(current_lat, current_lon, pending[idx]["latitude"], pending[idx]["longitude"]),
        )
        next_point = pending.pop(next_index)
        distance = haversine_km(current_lat, current_lon, next_point["latitude"], next_point["longitude"])
        route.append((stop, next_point, distance))
        current_lat = next_point["latitude"]
        current_lon = next_point["longitude"]
        stop += 1

    return route


def build_route_assignments(spark: SparkSession, data: Dict[str, DataFrame], cfg: AppConfig) -> DataFrame:
    orders = (
        data["orders"]
        .where(F.col("status").isin("completed", "pending", "in_progress"))
        .where(F.col("delivery_latitude").isNotNull() & F.col("delivery_longitude").isNotNull())
        .select("order_id", "user_id", "zone", "created_at", "delivery_latitude", "delivery_longitude")
        .orderBy("created_at", "order_id")
    )

    rows = [row.asDict() for row in orders.collect()]
    buckets: List[List[dict]] = [[] for _ in range(cfg.couriers)]
    for index, row in enumerate(rows):
        buckets[index % cfg.couriers].append({
            "order_id": row["order_id"],
            "user_id": row["user_id"],
            "zone": row["zone"] or "Sin zona",
            "latitude": float(row["delivery_latitude"]),
            "longitude": float(row["delivery_longitude"]),
        })

    output_rows = []
    for courier_index, bucket in enumerate(buckets, start=1):
        accumulated_km = 0.0
        courier_id = f"courier-{courier_index}"
        for stop_order, point, distance in nearest_neighbor_route(bucket, cfg.center_lat, cfg.center_lon):
            accumulated_km += distance
            output_rows.append((
                courier_id,
                stop_order,
                point["order_id"],
                point["user_id"],
                f"loc-{point['order_id']}",
                point["zone"],
                point["latitude"],
                point["longitude"],
                round(distance, 3),
                round(distance / 25.0 * 60.0, 2),
                round(accumulated_km, 3),
            ))

    schema = T.StructType([
        T.StructField("courier_id", T.StringType(), False),
        T.StructField("stop_order", T.IntegerType(), False),
        T.StructField("order_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("location_id", T.StringType(), False),
        T.StructField("zone", T.StringType(), True),
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("distance_from_previous_km", T.DoubleType(), True),
        T.StructField("estimated_minutes", T.DoubleType(), True),
        T.StructField("accumulated_km", T.DoubleType(), True),
    ])
    return spark.createDataFrame(output_rows, schema)


def write_csv_folder(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def write_single_csv(df: DataFrame, folder: str, filename: str) -> None:
    os.makedirs(folder, exist_ok=True)

    temp_dir = tempfile.mkdtemp(prefix=f"spark-export-{filename.replace('.csv', '')}-", dir="/tmp")

    try:
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(temp_dir)
        )

        part_files = [
            name for name in os.listdir(temp_dir)
            if name.startswith("part-") and name.endswith(".csv")
        ]

        if not part_files:
            raise RuntimeError(f"Spark no generó archivo part en {temp_dir}")

        final_path = os.path.join(folder, filename)
        temp_final_path = os.path.join(folder, f".{filename}.tmp")

        if os.path.exists(temp_final_path):
            os.remove(temp_final_path)

        shutil.copyfile(os.path.join(temp_dir, part_files[0]), temp_final_path)

        if os.path.exists(final_path):
            os.remove(final_path)

        os.replace(temp_final_path, final_path)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def export_neo4j_files(data: Dict[str, DataFrame], analysis: Dict[str, DataFrame], routes: DataFrame, cfg: AppConfig) -> None:
    users = data["users"].select("user_id", "name", "zone")
    products = data["products"].select("product_id", "name", "category", "price")
    orders = data["orders"].select("order_id", "user_id", "restaurant_id", F.date_format("created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"), "status", "zone")
    order_items = data["order_items"].select("order_id", "product_id", "quantity", "unit_price")

    recommendations = (
        data["users"]
        .where(F.col("referred_by").isNotNull())
        .select(F.col("referred_by").alias("from_user_id"), F.col("user_id").alias("to_user_id"))
    )

    center_schema = T.StructType([
        T.StructField("location_id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
    ])
    center = data["orders"].sparkSession.createDataFrame([
        ("loc-central", "Centro de distribución", cfg.center_lat, cfg.center_lon)
    ], center_schema)

    order_locations = routes.select(
        "location_id",
        F.concat(F.lit("Entrega "), F.col("order_id")).alias("name"),
        "latitude",
        "longitude",
    ).distinct()
    locations = center.unionByName(order_locations)

    route_edges = (
        routes.select(
            "courier_id",
            "stop_order",
            F.when(F.col("stop_order") == 1, F.lit("loc-central"))
            .otherwise(F.concat(F.lit("loc-"), F.lag("order_id").over(Window.partitionBy("courier_id").orderBy("stop_order"))))
            .alias("from_location_id"),
            "location_id",
            "distance_from_previous_km",
            "estimated_minutes",
        )
        .select(
            "courier_id",
            "stop_order",
            "from_location_id",
            F.col("location_id").alias("to_location_id"),
            "distance_from_previous_km",
            "estimated_minutes",
        )
    )

    exports = {
        "users.csv": users,
        "products.csv": products,
        "orders.csv": orders,
        "order_items.csv": order_items,
        "recommendations.csv": recommendations,
        "co_purchases.csv": analysis["co_purchases"],
        "locations.csv": locations,
        "route_assignments.csv": routes,
        "route_edges.csv": route_edges,
    }
    for filename, df in exports.items():
        write_single_csv(df, cfg.neo4j_output, filename)


def write_hive_tables(spark: SparkSession, analysis: Dict[str, DataFrame], routes: DataFrame, cfg: AppConfig) -> None:
    if not cfg.write_hive:
        print("[INFO] Escritura Hive omitida por --no-hive")
        return
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS restaurants_dw")
        for name, df in analysis.items():
            df.write.mode("overwrite").saveAsTable(f"restaurants_dw.{name}")
        routes.write.mode("overwrite").saveAsTable("restaurants_dw.route_assignments")
        print("[INFO] Tablas Hive generadas en restaurants_dw")
    except Exception as exc:
        print(f"[WARN] No se pudieron escribir tablas Hive: {exc}")
        print("[WARN] Los resultados CSV sí quedan disponibles en el directorio de salida")


def main() -> None:
    cfg = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Fuente: {cfg.source}")
    print(f"[INFO] Output analítico: {cfg.output_base}")
    print(f"[INFO] Output Neo4J: {cfg.neo4j_output}")

    raw_data = read_source(spark, cfg)
    data = normalize_sources(raw_data)
    analysis = run_analysis(spark, data)
    routes = build_route_assignments(spark, data, cfg)

    results_dir = os.path.join(cfg.output_base, "results")
    for name, df in analysis.items():
        write_csv_folder(df, os.path.join(results_dir, name))
    write_csv_folder(routes, os.path.join(results_dir, "route_assignments"))

    export_neo4j_files(data, analysis, routes, cfg)
    write_hive_tables(spark, analysis, routes, cfg)

    print("[OK] Job Spark finalizado correctamente")
    print("[OK] Resultados analíticos generados en:", results_dir)
    print("[OK] CSVs para Neo4J generados en:", cfg.neo4j_output)
    spark.stop()


if __name__ == "__main__":
    main()
