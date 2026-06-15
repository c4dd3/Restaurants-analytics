#!/bin/bash
# =============================================================================
# entrypoint.sh — Airflow
# Inicializa la base de datos, crea el usuario admin y arranca los procesos.
# =============================================================================

set -e

echo "=== Migrando base de datos de Airflow ==="
airflow db migrate

echo "=== Creando usuario admin (si no existe) ==="
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@restaurants.local 2>/dev/null || echo "Usuario ya existe, continuando..."

echo "=== Iniciando Airflow Webserver ==="
airflow webserver &

echo "=== Iniciando Airflow Scheduler ==="
exec airflow scheduler
