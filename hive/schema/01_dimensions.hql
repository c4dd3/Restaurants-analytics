-- =============================================================================
-- 01_dimensions.hql
-- Tablas de dimensión del esquema estrella.
-- Base de datos: restaurants_dw
-- Motor de almacenamiento: ORC (mejor rendimiento para consultas analíticas)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS restaurants_dw
  COMMENT 'Data Warehouse — Proyecto 2 BD2 TEC';

USE restaurants_dw;

-- -----------------------------------------------------------------------------
-- dim_tiempo
-- Generada por Spark a partir de los timestamps de orders y reservations.
-- Permite análisis por año, mes, semana, día y hora.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_tiempo (
  tiempo_key      BIGINT    COMMENT 'Llave surrogate: formato YYYYMMDDhh',
  fecha           DATE,
  anio            INT,
  trimestre       INT,
  mes             INT,
  nombre_mes      STRING,
  semana          INT,
  dia             INT,
  dia_semana      INT       COMMENT '1=Lunes … 7=Domingo',
  nombre_dia      STRING,
  hora            INT,
  es_fin_semana   BOOLEAN
)
COMMENT 'Dimensión de tiempo para análisis temporal'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- -----------------------------------------------------------------------------
-- dim_usuario
-- Un registro por usuario del Proyecto 1.
-- No incluye password ni email completo por privacidad.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_usuario (
  usuario_key     BIGINT    COMMENT 'Llave surrogate',
  usuario_id      STRING    COMMENT 'UUID del Proyecto 1',
  nombre          STRING,
  rol             STRING    COMMENT 'client | admin',
  fecha_registro  DATE
)
COMMENT 'Dimensión de usuarios'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- -----------------------------------------------------------------------------
-- dim_restaurante
-- Un registro por restaurante.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_restaurante (
  restaurante_key BIGINT    COMMENT 'Llave surrogate',
  restaurante_id  STRING    COMMENT 'UUID del Proyecto 1',
  nombre          STRING,
  direccion       STRING,
  capacidad       INT
)
COMMENT 'Dimensión de restaurantes'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- -----------------------------------------------------------------------------
-- dim_producto
-- Un registro por producto. Incluye categoría para análisis OLAP.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_producto (
  producto_key    BIGINT    COMMENT 'Llave surrogate',
  producto_id     STRING    COMMENT 'UUID del Proyecto 1',
  nombre          STRING,
  categoria       STRING,
  precio_actual   DECIMAL(10,2),
  disponible      BOOLEAN,
  restaurante_id  STRING    COMMENT 'Desnormalizado para facilitar joins'
)
COMMENT 'Dimensión de productos del menú'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
