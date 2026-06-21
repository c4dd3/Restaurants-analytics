# =============================================================================
# elasticsearch_loader.py
# Utilidades para interactuar con el servicio de búsqueda del Proyecto 1.
# El reindexado se hace a través del endpoint REST del search-service,
# no directamente contra ElasticSearch, para respetar la arquitectura del P1.
# =============================================================================

from __future__ import annotations

import requests
from airflow.models import Variable


# Endpoint del search-service del Proyecto 1 (accesible via re2_net)
REINDEX_URL   = "http://re2_nginx/search/reindex"
HEALTH_URL    = "http://re2_nginx/search/health"
TIMEOUT_SECS  = 30


def _get_auth_header() -> dict:
    """
    Obtiene el token de admin desde las Variables de Airflow.
    El token debe guardarse en Airflow UI → Admin → Variables
    con la clave 'api_admin_token'.
    """
    token = Variable.get("api_admin_token", default_var="")
    if not token:
        raise ValueError(
            "Variable 'api_admin_token' no configurada en Airflow. "
            "Ir a Admin → Variables y agregar el JWT de un usuario admin del Proyecto 1."
        )
    return {"Authorization": f"Bearer {token}"}


def check_search_service() -> bool:
    """
    Verifica que el search-service del Proyecto 1 esté disponible.
    Devuelve True si responde, lanza excepción si no.
    """
    try:
        response = requests.get(HEALTH_URL, timeout=10)
        response.raise_for_status()
        print(f"✓ Search service accesible — {response.json()}")
        return True
    except requests.RequestException as e:
        raise ConnectionError(f"Search service no disponible: {e}") from e


def trigger_reindex() -> dict:
    """
    Dispara el reindexado completo del catálogo de productos en ElasticSearch.
    Llama a POST /search/reindex del Proyecto 1 con autenticación de admin.

    Returns:
        dict: respuesta JSON del endpoint.
    """
    check_search_service()
    headers = _get_auth_header()

    print(f"→ Disparando reindexado en {REINDEX_URL}")
    response = requests.post(REINDEX_URL, headers=headers, timeout=TIMEOUT_SECS)

    if response.status_code == 401:
        raise PermissionError(
            "Token de admin inválido o expirado. "
            "Actualizar 'api_admin_token' en Airflow Variables."
        )

    response.raise_for_status()
    result = response.json()
    print(f"✓ Reindexado completado — {result}")
    return result
