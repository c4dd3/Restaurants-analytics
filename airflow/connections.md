# Conexiones y Variables de Airflow

Configurar en **Admin → Connections** y **Admin → Variables** de la UI de Airflow
(http://localhost:8085) antes de correr el DAG.

---

## Connections

### `postgres_proyecto1`
Conexión al Postgres del Proyecto 1.

| Campo      | Valor                  |
|------------|------------------------|
| Conn Id    | `postgres_proyecto1`   |
| Conn Type  | `Postgres`             |
| Host       | `re2_postgres`         |
| Schema     | `restaurants`          |
| Login      | `postgres`             |
| Password   | `postgres`             |
| Port       | `5432`                 |

---

### `spark_default`
Conexión al Spark Master.

| Campo      | Valor                        |
|------------|------------------------------|
| Conn Id    | `spark_default`              |
| Conn Type  | `Spark`                      |
| Host       | `spark://spark-master`       |
| Port       | `7077`                       |

---

## Variables

### `api_admin_token`
JWT de un usuario admin del Proyecto 1. Se usa para llamar a `POST /search/reindex`.

**Cómo obtenerlo:**
```bash
# Con el Proyecto 1 levantado:
curl -s -X POST http://localhost/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"tu_password"}' \
  | jq -r '.token'
```
Copiar el token y guardarlo en Airflow UI → Admin → Variables → clave: `api_admin_token`.

---

### `products_catalog_hash`
Generada automáticamente por el DAG en la primera ejecución. No es necesario crearla manualmente.

---

## Setup rápido vía CLI (alternativa a la UI)

```bash
# Conexión Postgres
docker exec ra_airflow airflow connections add postgres_proyecto1 \
  --conn-type postgres \
  --conn-host re2_postgres \
  --conn-schema restaurants \
  --conn-login postgres \
  --conn-password postgres \
  --conn-port 5432

# Conexión Spark
docker exec ra_airflow airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077

# Variable del token (reemplazar TOKEN por el valor real)
docker exec ra_airflow airflow variables set api_admin_token TOKEN
```
