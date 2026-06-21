#!/usr/bin/env python3
"""
seed_transactions.py
Genera órdenes, order_items y reservaciones en el Postgres del Proyecto 1
usando los usuarios, restaurantes y productos ya existentes.

Uso:
    python3 scripts/seed_transactions.py
    python3 scripts/seed_transactions.py --orders 200 --reservations 100
"""

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta

import psycopg2

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB",   "restaurants")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PWD  = os.getenv("POSTGRES_PASSWORD", "postgres")

STATUSES_ORDER = ["pending", "confirmed", "cancelled"]
STATUSES_ORDER_WEIGHTS = [0.1, 0.75, 0.15]

STATUSES_RESERVATION = ["pending", "confirmed", "cancelled"]
STATUSES_RESERVATION_WEIGHTS = [0.1, 0.75, 0.15]


def random_ts(days_back: int = 180) -> datetime:
    """Timestamp aleatorio en los últimos N días."""
    now = datetime.now()
    delta = random.randint(0, days_back * 24 * 60)
    return now - timedelta(minutes=delta)


def seed(conn, n_orders: int, n_reservations: int) -> None:
    cur = conn.cursor()

    # Leer entidades existentes
    cur.execute("SELECT id FROM users WHERE role = 'client'")
    users = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM users WHERE role = 'admin'")
    admins = [r[0] for r in cur.fetchall()]
    all_users = users + admins

    cur.execute("SELECT id FROM restaurants")
    restaurants = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id, restaurant_id, price FROM products WHERE available = true")
    products = cur.fetchall()   # (id, restaurant_id, price)

    if not all_users or not restaurants or not products:
        print("[ERROR] No hay usuarios, restaurantes o productos. Corre el seed base primero.")
        return

    # Agrupar productos por restaurante
    products_by_restaurant: dict[str, list] = {}
    for pid, rid, price in products:
        products_by_restaurant.setdefault(str(rid), []).append((str(pid), float(price)))

    print(f"[INFO] Encontrados: {len(all_users)} usuarios, {len(restaurants)} restaurantes, {len(products)} productos")

    # ------------------------------------------------------------------
    # Órdenes + order_items
    # ------------------------------------------------------------------
    print(f"[INFO] Generando {n_orders} órdenes...")
    orders_inserted = 0
    items_inserted  = 0

    for _ in range(n_orders):
        restaurant_id = str(random.choice(restaurants))
        menu = products_by_restaurant.get(restaurant_id)
        if not menu:
            continue

        user_id   = str(random.choice(all_users))
        status    = random.choices(STATUSES_ORDER, STATUSES_ORDER_WEIGHTS)[0]
        pickup    = random.random() < 0.3
        created   = random_ts(180)
        order_id  = str(uuid.uuid4())

        # 1-4 items por orden
        n_items = random.randint(1, 4)
        selected = random.sample(menu, min(n_items, len(menu)))
        total = sum(price * random.randint(1, 3) for _, price in selected)

        cur.execute(
            """
            INSERT INTO orders (id, user_id, restaurant_id, total, status, pickup, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (order_id, user_id, restaurant_id, round(total, 2), status, pickup, created),
        )
        orders_inserted += 1

        for product_id, price in selected:
            qty = random.randint(1, 3)
            cur.execute(
                """
                INSERT INTO order_items (id, order_id, product_id, quantity, price)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (str(uuid.uuid4()), order_id, product_id, qty, round(price, 2)),
            )
            items_inserted += 1

    # ------------------------------------------------------------------
    # Reservaciones
    # ------------------------------------------------------------------
    print(f"[INFO] Generando {n_reservations} reservaciones...")
    reservations_inserted = 0

    for _ in range(n_reservations):
        restaurant_id = str(random.choice(restaurants))
        user_id       = str(random.choice(all_users))
        status        = random.choices(STATUSES_RESERVATION, STATUSES_RESERVATION_WEIGHTS)[0]
        party_size    = random.randint(1, 8)
        date          = random_ts(180)
        created       = date - timedelta(hours=random.randint(1, 48))

        cur.execute(
            """
            INSERT INTO reservations (id, restaurant_id, user_id, date, party_size, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (str(uuid.uuid4()), restaurant_id, user_id, date, party_size, status, created),
        )
        reservations_inserted += 1

    conn.commit()
    cur.close()

    print(f"""
✅ Seed de transacciones completado:
   Órdenes:        {orders_inserted}
   Order items:    {items_inserted}
   Reservaciones:  {reservations_inserted}
""")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed de transacciones para Proyecto 1")
    parser.add_argument("--orders",       type=int, default=300, help="Número de órdenes a generar")
    parser.add_argument("--reservations", type=int, default=150, help="Número de reservaciones a generar")
    args = parser.parse_args()

    print(f"[INFO] Conectando a {PG_HOST}:{PG_PORT}/{PG_DB}...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PWD,
    )
    try:
        seed(conn, args.orders, args.reservations)
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
