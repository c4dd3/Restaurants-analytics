# =============================================================================
# postgres_extractor.py
# Utilidades para extraer datos del Postgres del Proyecto 1 desde los DAGs.
# =============================================================================

from __future__ import annotations

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


PG_CONN_ID = "postgres_proyecto1"


def get_hook() -> PostgresHook:
    """Devuelve un hook conectado al Postgres del Proyecto 1."""
    return PostgresHook(postgres_conn_id=PG_CONN_ID)


def query_to_df(sql: str) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL en el Postgres del Proyecto 1
    y devuelve el resultado como DataFrame de pandas.

    Ejemplo:
        df = query_to_df("SELECT * FROM orders WHERE status = 'confirmed'")
    """
    hook = get_hook()
    return hook.get_pandas_df(sql)


def get_row_count(table: str) -> int:
    """Devuelve el número de filas de una tabla."""
    hook = get_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table};")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def get_products_snapshot() -> list[tuple]:
    """
    Devuelve todos los productos ordenados por id.
    Usado por el DAG para detectar cambios en el catálogo.
    """
    hook = get_hook()
    conn   = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, category, price, available
        FROM products
        ORDER BY id;
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


# Consultas predefinidas útiles para los DAGs
QUERIES = {
    "orders_with_items": """
        SELECT
            o.id           AS order_id,
            o.user_id,
            o.restaurant_id,
            o.reservation_id,
            o.total,
            o.status       AS order_status,
            o.pickup,
            o.created_at   AS order_created_at,
            oi.id          AS item_id,
            oi.product_id,
            oi.quantity,
            oi.price       AS item_price
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
    """,

    "reservations": """
        SELECT
            id,
            restaurant_id,
            user_id,
            date,
            party_size,
            status,
            created_at
        FROM reservations
    """,

    "users": """
        SELECT id, name, role, created_at AS fecha_registro
        FROM users
    """,

    "restaurants": """
        SELECT id, name, address, capacity
        FROM restaurants
    """,

    "products": """
        SELECT
            p.id,
            p.name,
            p.category,
            p.price,
            p.available,
            p.restaurant_id
        FROM products p
    """,
}
