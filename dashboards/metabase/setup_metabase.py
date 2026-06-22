#!/usr/bin/env python3
"""
setup_metabase.py
Automatiza la configuración inicial de Metabase para el proyecto Restaurants-analytics.

Pasos:
  1. Completa el setup inicial (si es primera vez) y crea usuario admin
  2. Agrega HiveServer2 como fuente de datos
  3. Crea 3 dashboards con las vistas OLAP

Uso:
  python3 dashboards/metabase/setup_metabase.py

Requisitos:
  pip install requests
"""

from __future__ import annotations
import sys
import time
import requests

METABASE_URL = "http://localhost:3000"
ADMIN_EMAIL    = "admin@restaurants.local"
ADMIN_PASSWORD = "Admin1234!"
ADMIN_FIRSTNAME = "Admin"
ADMIN_LASTNAME  = "Restaurants"
SITE_NAME       = "Restaurants Analytics"

HIVE_HOST = "hive-server"   # nombre del servicio en docker (dentro de ra_net)
HIVE_PORT = 10000

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_metabase(max_wait=120):
    print("⏳ Esperando que Metabase esté disponible...")
    start = time.time()
    while time.time() - start < max_wait:
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("✓ Metabase está listo")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    print("✗ Metabase no respondió a tiempo")
    return False


def get_setup_token() -> str | None:
    r = requests.get(f"{METABASE_URL}/api/session/properties")
    r.raise_for_status()
    return r.json().get("setup-token")


def is_already_setup() -> bool:
    token = get_setup_token()
    return token is None


def complete_setup(setup_token: str) -> str:
    """Completa el wizard de primer arranque; retorna el session token."""
    print("🔧 Completando setup inicial de Metabase...")
    payload = {
        "token": setup_token,
        "prefs": {"site_name": SITE_NAME, "site_locale": "es"},
        "database": None,
        "user": {
            "first_name":    ADMIN_FIRSTNAME,
            "last_name":     ADMIN_LASTNAME,
            "email":         ADMIN_EMAIL,
            "password":      ADMIN_PASSWORD,
            "site_name":     SITE_NAME,
        },
    }
    r = requests.post(f"{METABASE_URL}/api/setup", json=payload)
    if r.status_code not in (200, 201):
        print(f"  ✗ Setup falló: {r.status_code} {r.text}")
        sys.exit(1)
    token = r.json().get("id")
    print("  ✓ Setup completado")
    return token


def login() -> str:
    """Inicia sesión y retorna el session token."""
    r = requests.post(f"{METABASE_URL}/api/session",
                      json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    if r.status_code == 200:
        return r.json()["id"]
    print(f"  ✗ Login falló: {r.status_code} {r.text}")
    sys.exit(1)


class MetabaseClient:
    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "X-Metabase-Session": token,
            "Content-Type": "application/json",
        })

    def get(self, path, **kw):
        return self.session.get(f"{METABASE_URL}{path}", **kw)

    def post(self, path, **kw):
        return self.session.post(f"{METABASE_URL}{path}", **kw)

    # ── Database ──────────────────────────────────────────────────────────────

    def list_engines(self) -> list[str]:
        r = self.get("/api/database/db_engines")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return list(data.keys())
            return data
        # fallback: list databases and check what's available
        return []

    def list_databases(self) -> list[dict]:
        r = self.get("/api/database")
        r.raise_for_status()
        return r.json().get("data", r.json())

    def find_or_create_hive_db(self) -> int:
        # Check if already exists
        for db in self.list_databases():
            if db.get("engine") in ("hive", "sparksql") or \
               "hive" in db.get("name", "").lower():
                print(f"  ✓ Base de datos Hive ya existe (id={db['id']})")
                return db["id"]

        # Try hive engine first, then sparksql, then spark
        for engine in ("hive", "sparksql", "spark"):
            db_id = self._try_create_db(engine)
            if db_id:
                return db_id

        print("  ✗ No se pudo conectar a Hive con ningún driver disponible.")
        print("    Conecta manualmente en Metabase: Settings → Databases → Add database")
        print(f"    Host: {HIVE_HOST}, Puerto: {HIVE_PORT}, Base de datos: restaurants_dw")
        return -1

    def _try_create_db(self, engine: str) -> int | None:
        details: dict = {
            "host": HIVE_HOST,
            "port": HIVE_PORT,
            "dbname": "restaurants_dw",
        }
        if engine == "sparksql":
            details["host"]   = HIVE_HOST
            details["port"]   = HIVE_PORT
            details["dbname"] = "restaurants_dw"

        payload = {
            "engine":  engine,
            "name":    "Restaurants DW (Hive)",
            "details": details,
            "auto_run_queries": False,
        }
        r = self.post("/api/database", json=payload)
        if r.status_code in (200, 201):
            db_id = r.json()["id"]
            print(f"  ✓ Base de datos creada con engine='{engine}' (id={db_id})")
            return db_id
        # 400 usually means engine not found or bad config — try next
        return None

    def sync_db(self, db_id: int):
        self.post(f"/api/database/{db_id}/sync_schema")
        print(f"  ✓ Sync iniciado para db_id={db_id} (espera ~30 s antes de crear preguntas)")

    # ── Cards (Questions) ─────────────────────────────────────────────────────

    def create_card(self, name: str, db_id: int, sql: str,
                    display: str = "table") -> int:
        # Return existing card if already created
        for c in self.list_cards():
            if c.get("name") == name:
                print(f"    ✓ Pregunta ya existe: '{name}' (id={c['id']})")
                return c["id"]
        payload = {
            "name":             name,
            "display":          display,
            "visualization_settings": {},
            "dataset_query": {
                "type":     "native",
                "database": db_id,
                "native":   {"query": sql},
            },
            "collection_id": None,
        }
        r = self.post("/api/card", json=payload)
        if r.status_code in (200, 201):
            card_id = r.json()["id"]
            print(f"    ✓ Pregunta creada: '{name}' (id={card_id})")
            return card_id
        print(f"    ✗ Error creando '{name}': {r.status_code} {r.text[:200]}")
        return -1

    # ── Dashboards ────────────────────────────────────────────────────────────

    def list_dashboards(self) -> list[dict]:
        r = self.get("/api/dashboard")
        if r.status_code == 200:
            return r.json()
        return []

    def list_cards(self) -> list[dict]:
        r = self.get("/api/card")
        if r.status_code == 200:
            return r.json()
        return []

    def create_dashboard(self, name: str, description: str) -> int:
        # Return existing dashboard if already created
        for d in self.list_dashboards():
            if d.get("name") == name:
                print(f"  ✓ Dashboard ya existe: '{name}' (id={d['id']})")
                return d["id"]
        r = self.post("/api/dashboard",
                      json={"name": name, "description": description})
        if r.status_code in (200, 201):
            dash_id = r.json()["id"]
            print(f"  ✓ Dashboard creado: '{name}' (id={dash_id})")
            return dash_id
        print(f"  ✗ Error creando dashboard '{name}': {r.status_code} {r.text[:200]}")
        return -1

    def add_card_to_dashboard(self, dashboard_id: int, card_id: int,
                               row: int = 0, col: int = 0,
                               size_x: int = 18, size_y: int = 8):
        # v0.61+: PUT /api/dashboard/:id — send full dashcards list.
        # Existing dashcards keep their real id; new ones use id=-1.
        dash = self.get(f"/api/dashboard/{dashboard_id}").json()
        existing = dash.get("dashcards", [])

        # Skip if this card is already on the dashboard
        if any(dc.get("card_id") == card_id for dc in existing):
            print(f"    ✓ Pregunta ya está en el dashboard")
            return

        new_card = {
            "id":          -1,
            "card_id":     card_id,
            "row":         row,
            "col":         col,
            "size_x":      size_x,
            "size_y":      size_y,
            "series":      [],
            "parameter_mappings":     [],
            "visualization_settings": {},
        }
        r = self.session.put(
            f"{METABASE_URL}/api/dashboard/{dashboard_id}",
            json={"dashcards": existing + [new_card]},
        )
        if r.status_code in (200, 201):
            print(f"    ✓ Pregunta agregada al dashboard")
        else:
            print(f"    ✗ Error agregando al dashboard: {r.status_code} {r.text[:300]}")


# ─────────────────────────────────────────────────────────────────────────────
# Definición de los dashboards y sus preguntas OLAP
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARDS = [
    {
        "name": "Ingresos por Mes y Categoría",
        "description": "Evolución mensual de ingresos y unidades vendidas por categoría de producto.",
        "cards": [
            {
                "name": "Ingresos totales por mes y categoría",
                "display": "bar",
                "sql": """
SELECT nombre_mes, categoria,
       ingresos_totales, unidades_vendidas, total_pedidos
FROM restaurants_dw.olap_ingresos_por_mes_categoria
ORDER BY anio, mes, categoria
""",
            },
            {
                "name": "Ticket promedio por categoría",
                "display": "bar",
                "sql": """
SELECT categoria,
       ROUND(AVG(ticket_promedio), 2) AS ticket_promedio_general,
       SUM(ingresos_totales)          AS ingresos_acumulados
FROM restaurants_dw.olap_ingresos_por_mes_categoria
GROUP BY categoria
ORDER BY ingresos_acumulados DESC
""",
            },
        ],
    },
    {
        "name": "Actividad de Clientes por Zona",
        "description": "Usuarios únicos, pedidos e ingresos por restaurante/zona geográfica.",
        "cards": [
            {
                "name": "Usuarios únicos por restaurante",
                "display": "bar",
                "sql": """
SELECT restaurante, zona,
       SUM(usuarios_unicos) AS usuarios_unicos_total,
       SUM(total_pedidos)   AS pedidos_total,
       SUM(ingresos)        AS ingresos_total
FROM restaurants_dw.olap_actividad_usuarios_por_restaurante
GROUP BY restaurante, zona
ORDER BY pedidos_total DESC
""",
            },
            {
                "name": "Actividad mensual por restaurante",
                "display": "line",
                "sql": """
SELECT nombre_mes, restaurante,
       usuarios_unicos, total_pedidos, ingresos
FROM restaurants_dw.olap_actividad_usuarios_por_restaurante
ORDER BY anio, mes, restaurante
""",
            },
        ],
    },
    {
        "name": "Estado de Pedidos",
        "description": "Distribución y tendencia de pedidos completados vs cancelados.",
        "cards": [
            {
                "name": "Pedidos por estado (totales)",
                "display": "pie",
                "sql": """
SELECT estado_pedido,
       SUM(total_pedidos) AS pedidos,
       SUM(monto_total)   AS monto_total
FROM restaurants_dw.olap_estado_pedidos
GROUP BY estado_pedido
ORDER BY pedidos DESC
""",
            },
            {
                "name": "Evolución mensual de estados de pedido",
                "display": "bar",
                "sql": """
SELECT nombre_mes, estado_pedido,
       total_pedidos, monto_total, porcentaje
FROM restaurants_dw.olap_estado_pedidos
ORDER BY anio, mes, estado_pedido
""",
            },
        ],
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if not wait_for_metabase():
        sys.exit(1)

    # ── Setup o login ──────────────────────────────────────────────────────────
    setup_token = get_setup_token()
    if setup_token:
        session_token = complete_setup(setup_token)
    else:
        print("ℹ Metabase ya fue configurado — iniciando sesión...")
        session_token = login()
        print("  ✓ Sesión iniciada")

    mb = MetabaseClient(session_token)

    # ── Base de datos Hive ────────────────────────────────────────────────────
    print("\n📦 Configurando conexión a HiveServer2...")
    db_id = mb.find_or_create_hive_db()
    if db_id == -1:
        print("\n⚠ Crea la conexión a Hive manualmente y vuelve a correr el script")
        print("  Luego podrás correr solo la parte de dashboards.")
        sys.exit(1)

    mb.sync_db(db_id)
    print("  ⏳ Esperando 35 s para que el schema sync termine...")
    time.sleep(35)

    # ── Dashboards ─────────────────────────────────────────────────────────────
    print("\n📊 Creando dashboards...")
    for dash_def in DASHBOARDS:
        print(f"\n  → {dash_def['name']}")
        dash_id = mb.create_dashboard(dash_def["name"], dash_def["description"])
        if dash_id == -1:
            continue

        row = 0
        for card_def in dash_def["cards"]:
            card_id = mb.create_card(
                name    = card_def["name"],
                db_id   = db_id,
                sql     = card_def["sql"].strip(),
                display = card_def.get("display", "table"),
            )
            if card_id != -1:
                mb.add_card_to_dashboard(dash_id, card_id, row=row)
                row += 8

    print(f"\n✅ Listo. Abre Metabase en http://localhost:3000")
    print(f"   Email:    {ADMIN_EMAIL}")
    print(f"   Password: {ADMIN_PASSWORD}")


if __name__ == "__main__":
    main()
