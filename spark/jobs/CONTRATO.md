# Contrato de interfaces — Jobs de Spark

Este archivo define exactamente qué debe producir cada job de Spark.
**No modificar sin coordinarse con Persona A (Hive + Airflow).**

---

## Base de datos destino en Hive

Todos los jobs escriben a la base de datos `restaurants_dw` en Hive.  
Metastore URI: `thrift://hive-metastore:9083` (ya configurado en `spark/conf/hive-site.xml`).

---

## Fuentes de datos disponibles (Proyecto 1)

```python
# PostgreSQL
PG_URL  = "jdbc:postgresql://re2_postgres:5432/restaurants"
PG_PROPS = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

# MongoDB
MONGO_URI = "mongodb://re2_mongos:27017/restaurants"
```

Tablas en Postgres: `users`, `restaurants`, `menus`, `products`, `reservations`, `orders`, `order_items`

---

## Jobs requeridos

### Job 1 — `cargar_dimensiones.py`

**Responsabilidad:** Leer usuarios, restaurantes y productos del Proyecto 1,
generar llaves surrogate e insertar/sobreescribir las tablas de dimensión.

**Escribe a:**

| Tabla Hive          | Columnas clave                                                                |
|---------------------|-------------------------------------------------------------------------------|
| `dim_usuario`       | usuario_key, usuario_id, nombre, rol, fecha_registro                          |
| `dim_restaurante`   | restaurante_key, restaurante_id, nombre, direccion, capacidad                 |
| `dim_producto`      | producto_key, producto_id, nombre, categoria, precio_actual, disponible, restaurante_id |
| `dim_tiempo`        | tiempo_key, fecha, anio, trimestre, mes, nombre_mes, semana, dia, dia_semana, nombre_dia, hora, es_fin_semana |

**`tiempo_key`:** formato numérico `YYYYMMDDhh` (ej: `2024011514` = 15 ene 2024, 14:00h).  
**`*_key` surrogate:** usar `monotonically_increasing_id()` de Spark o hash del UUID.

**Modo de escritura:** `overwrite` (se recarga completo cada ejecución).

---

### Job 2 — `cargar_fact_items_pedido.py`

**Responsabilidad:** Leer `orders` + `order_items` del Proyecto 1, hacer join con
las dimensiones ya cargadas y escribir la tabla de hechos principal.

**Lee de Hive:** `dim_tiempo`, `dim_usuario`, `dim_restaurante`, `dim_producto`  
**Escribe a Hive:** `fact_items_pedido`

| Columna           | Tipo            | Fuente                              |
|-------------------|-----------------|-------------------------------------|
| tiempo_key        | BIGINT          | `dim_tiempo` via `orders.created_at` |
| usuario_key       | BIGINT          | `dim_usuario` via `orders.user_id`  |
| restaurante_key   | BIGINT          | `dim_restaurante` via `orders.restaurant_id` |
| producto_key      | BIGINT          | `dim_producto` via `order_items.product_id` |
| pedido_id         | STRING          | `orders.id`                         |
| item_id           | STRING          | `order_items.id`                    |
| cantidad          | INT             | `order_items.quantity`              |
| precio_unitario   | DECIMAL(10,2)   | `order_items.price`                 |
| monto_total       | DECIMAL(10,2)   | `quantity * price`                  |
| estado_pedido     | STRING          | `orders.status`                     |
| es_para_llevar    | BOOLEAN         | `orders.pickup`                     |

**Modo de escritura:** `overwrite`.

---

### Job 3 — `cargar_fact_reservaciones.py`

**Responsabilidad:** Leer `reservations` del Proyecto 1 y escribir la tabla de hechos
de reservaciones.

**Lee de Hive:** `dim_tiempo`, `dim_usuario`, `dim_restaurante`  
**Escribe a Hive:** `fact_reservaciones`

| Columna           | Tipo    | Fuente                                        |
|-------------------|---------|-----------------------------------------------|
| tiempo_key        | BIGINT  | `dim_tiempo` via `reservations.date`          |
| usuario_key       | BIGINT  | `dim_usuario` via `reservations.user_id`      |
| restaurante_key   | BIGINT  | `dim_restaurante` via `reservations.restaurant_id` |
| reservacion_id    | STRING  | `reservations.id`                             |
| tamano_grupo      | INT     | `reservations.party_size`                     |
| estado            | STRING  | `reservations.status`                         |

**Modo de escritura:** `overwrite`.

---

## Orden de ejecución

```
1. cargar_dimensiones.py        ← primero siempre (las facts dependen de las dims)
2. cargar_fact_items_pedido.py  ← después de dimensiones
3. cargar_fact_reservaciones.py ← después de dimensiones (paralelo con Job 2)
```

El DAG de Airflow respeta este orden. Los jobs 2 y 3 pueden correr en paralelo.

---

## Template base para cada job

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("nombre_del_job")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate())

spark.sql("USE restaurants_dw")

# Leer de Postgres
df = spark.read.jdbc(
    url="jdbc:postgresql://re2_postgres:5432/restaurants",
    table="nombre_tabla",
    properties={"user": "postgres", "password": "postgres",
                "driver": "org.postgresql.Driver"}
)

# ... transformaciones ...

# Escribir a Hive
df_final.write.mode("overwrite").saveAsTable("restaurants_dw.nombre_tabla_hive")

spark.stop()
```

---

## Preguntas o cambios

Coordinarse con Persona A antes de cambiar nombres de tablas, columnas o tipos de datos.
