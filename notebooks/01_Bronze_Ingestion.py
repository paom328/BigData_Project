# Databricks notebook source
# DBTITLE 1,Import libraries and define parameters
# =============================================================================
# 01_Bronze_Ingestion.py
# Capa BRONZE: Ingesta de archivos CSV desde Unity Catalog Volumes
# hacia tablas Delta sin transformar.
# =============================================================================
# Pipeline: CSV -> Volume -> Bronze Delta Tables
# Frecuencia: Diaria 05:00 UTC (via Databricks Job)
# =============================================================================

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Parameters (can be overridden by Job)
dbutils.widgets.text("catalog_name", "catalog_logistica", "Catalog Name")
dbutils.widgets.text("schema_name", "db_rollos", "Schema Name")
dbutils.widgets.text("volume_path", "/Volumes/catalog_logistica/db_rollos/landing", "Volume Path")
dbutils.widgets.text("file_modelo_bigdata", "Modelo_bigdata.csv", "Archivo Modelo Bigdata")
dbutils.widgets.text("file_stock_wompi", "Stock_Wompi_Rollos_2026-09-12.csv", "Archivo Stock Wompi")
dbutils.widgets.text("file_oc_agosto", "OC_AGOSTO.csv", "Archivo OC Agosto")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_PATH = dbutils.widgets.get("volume_path")
FILE_MODELO_BIGDATA = dbutils.widgets.get("file_modelo_bigdata")
FILE_STOCK_WOMPI = dbutils.widgets.get("file_stock_wompi")
FILE_OC_AGOSTO = dbutils.widgets.get("file_oc_agosto")

FULL_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_NAME}"

print(f"[BRONZE] Catalog: {CATALOG_NAME}")
print(f"[BRONZE] Schema: {SCHEMA_NAME}")
print(f"[BRONZE] Volume Path: {VOLUME_PATH}")
print(f"[BRONZE] File Modelo: {FILE_MODELO_BIGDATA}")
print(f"[BRONZE] File Stock: {FILE_STOCK_WOMPI}")
print(f"[BRONZE] File OC: {FILE_OC_AGOSTO}")

# COMMAND ----------

# DBTITLE 1,Create catalog and schema if not exist
# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

print(f"[BRONZE] Catalog y schema verificados: {FULL_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Load Modelo_bigdata.xlsx -> bronze_modelo_bigdata
# =============================================================================
# 1. Read Modelo_bigdata.xlsx -> bronze_modelo_bigdata
# Maestro histórico de PUS (24,382 registros en producción)
# =============================================================================
modelo_bigdata_path = f"{VOLUME_PATH}/{FILE_MODELO_BIGDATA}"
print(f"[BRONZE] Leyendo: {modelo_bigdata_path}")

df_modelo_bigdata = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(modelo_bigdata_path)
)

# Normalize column names (remove spaces, accents)
columns_renamed = []
for col_name in df_modelo_bigdata.columns:
    clean_name = col_name.strip().upper().replace(" ", "_").replace("\n", "_")
    columns_renamed.append(clean_name)

df_modelo_bigdata = df_modelo_bigdata.toDF(*columns_renamed)

# Add ingestion metadata
df_modelo_bigdata = (
    df_modelo_bigdata
    .withColumn("_bronze_ingestion_date", current_timestamp())
    .withColumn("_bronze_source_file", lit(FILE_MODELO_BIGDATA))
    .withColumn("_bronze_layer", lit("BRONZE"))
)

print(f"[BRONZE] Modelo_bigdata: {df_modelo_bigdata.count()} registros")
print(f"[BRONZE] Columnas: {df_modelo_bigdata.columns}")
df_modelo_bigdata.printSchema()

# Write to Delta Bronze table
TABLE_BRONZE_MODELO = f"{FULL_SCHEMA}.bronze_modelo_bigdata"

(
    df_modelo_bigdata.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(TABLE_BRONZE_MODELO)
)

print(f"[BRONZE] Tabla escrita: {TABLE_BRONZE_MODELO}")
display(spark.sql(f"SELECT COUNT(*) as total FROM {TABLE_BRONZE_MODELO}"))

# COMMAND ----------

# DBTITLE 1,Load Stock_Wompi_Rollos -> bronze_stock_wompi
# =============================================================================
# 2. Read Stock_Wompi_Rollos_2026-09-12.xlsx -> bronze_stock_wompi
# Inventario real reportado por cncodpus
# =============================================================================
stock_wompi_path = f"{VOLUME_PATH}/{FILE_STOCK_WOMPI}"
print(f"[BRONZE] Leyendo: {stock_wompi_path}")

df_stock_wompi = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(stock_wompi_path)
)

# Normalize column names
columns_renamed_stock = []
for col_name in df_stock_wompi.columns:
    clean_name = col_name.strip().upper().replace(" ", "_").replace("\n", "_")
    columns_renamed_stock.append(clean_name)

df_stock_wompi = df_stock_wompi.toDF(*columns_renamed_stock)

# Add ingestion metadata
df_stock_wompi = (
    df_stock_wompi
    .withColumn("_bronze_ingestion_date", current_timestamp())
    .withColumn("_bronze_source_file", lit(FILE_STOCK_WOMPI))
    .withColumn("_bronze_layer", lit("BRONZE"))
)

print(f"[BRONZE] Stock_Wompi: {df_stock_wompi.count()} registros")
print(f"[BRONZE] Columnas: {df_stock_wompi.columns}")
df_stock_wompi.printSchema()

# Write to Delta Bronze table
TABLE_BRONZE_STOCK = f"{FULL_SCHEMA}.bronze_stock_wompi"

(
    df_stock_wompi.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(TABLE_BRONZE_STOCK)
)

print(f"[BRONZE] Tabla escrita: {TABLE_BRONZE_STOCK}")
display(spark.sql(f"SELECT COUNT(*) as total FROM {TABLE_BRONZE_STOCK}"))

# COMMAND ----------

# DBTITLE 1,Load OC AGOSTO.xlsx -> bronze_oc_agosto
# =============================================================================
# 3. Read OC AGOSTO.xlsx -> bronze_oc_agosto
# Requerimientos activos identificados por código de comercio (CB)
# =============================================================================
oc_agosto_path = f"{VOLUME_PATH}/{FILE_OC_AGOSTO}"
print(f"[BRONZE] Leyendo: {oc_agosto_path}")

df_oc_agosto = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(oc_agosto_path)
)

# Normalize column names
columns_renamed_oc = []
for col_name in df_oc_agosto.columns:
    clean_name = col_name.strip().upper().replace(" ", "_").replace("\n", "_")
    columns_renamed_oc.append(clean_name)

df_oc_agosto = df_oc_agosto.toDF(*columns_renamed_oc)

# Add ingestion metadata
df_oc_agosto = (
    df_oc_agosto
    .withColumn("_bronze_ingestion_date", current_timestamp())
    .withColumn("_bronze_source_file", lit(FILE_OC_AGOSTO))
    .withColumn("_bronze_layer", lit("BRONZE"))
)

print(f"[BRONZE] OC_Agosto: {df_oc_agosto.count()} registros")
print(f"[BRONZE] Columnas: {df_oc_agosto.columns}")
df_oc_agosto.printSchema()

# Write to Delta Bronze table
TABLE_BRONZE_OC = f"{FULL_SCHEMA}.bronze_oc_agosto"

(
    df_oc_agosto.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(TABLE_BRONZE_OC)
)

print(f"[BRONZE] Tabla escrita: {TABLE_BRONZE_OC}")
display(spark.sql(f"SELECT COUNT(*) as total FROM {TABLE_BRONZE_OC}"))

# COMMAND ----------

# DBTITLE 1,Bronze ingestion summary
# =============================================================================
# Resumen y validación de ingesta Bronze
# =============================================================================
print("=" * 70)
print("[BRONZE] RESUMEN DE INGESTA")
print("=" * 70)

bronze_tables = [
    ("bronze_modelo_bigdata", TABLE_BRONZE_MODELO),
    ("bronze_stock_wompi", TABLE_BRONZE_STOCK),
    ("bronze_oc_agosto", TABLE_BRONZE_OC),
]

for table_name, full_name in bronze_tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").collect()[0]["cnt"]
    print(f"  {table_name}: {count:,} registros -> {full_name}")

print("=" * 70)
print("[BRONZE] Ingesta completada exitosamente.")
print("=" * 70)

# COMMAND ----------

