#!/bin/bash
# =============================================================================
# init-multiple-dbs.sh
# Crea las bases de datos necesarias para el stack de analítica.
# Postgres ya crea `hive_metastore` como POSTGRES_DB; este script agrega
# `airflow` y `metabase` en el mismo servidor.
# =============================================================================

set -e

OWNER="${POSTGRES_USER:-analytics}"

create_db_if_not_exists() {
  local DB=$1
  echo "  → Verificando base de datos: $DB"
  psql -v ON_ERROR_STOP=1 --username "$OWNER" <<-EOSQL
    SELECT 'CREATE DATABASE "$DB" OWNER "$OWNER"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB')\gexec
EOSQL
}

echo "=== Inicializando bases de datos de analítica ==="
create_db_if_not_exists "airflow"
create_db_if_not_exists "metabase"
echo "=== Listo ==="
