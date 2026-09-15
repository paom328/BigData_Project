# Databricks notebook source
# DBTITLE 1,Import libraries and define parameters
# =============================================================================
# 03_Gold_ML_Suggestions.py
# Capa GOLD: Machine Learning + Sugerencias de abastecimiento
# =============================================================================
# Pipeline: Silver -> Gold (ML forecast + lote optimo + sugerencias)
# Modelo: RandomForestRegressor (100 trees, maxDepth=10)
# Horizonte: 30 dias | Cobertura objetivo: 60 dias
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
import math

# Parameters
dbutils.widgets.text("catalog_name", "catalog_logistica", "Catalog Name")
dbutils.widgets.text("schema_name", "db_rollos", "Schema Name")
dbutils.widgets.text("forecast_horizon_days", "30", "Forecast Horizon (days)")
dbutils.widgets.text("target_coverage_days", "60", "Target Coverage (days)")
dbutils.widgets.text("stock_security_rollos", "30", "Stock Security (rolls)")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
FORECAST_HORIZON = int(dbutils.widgets.get("forecast_horizon_days"))
TARGET_COVERAGE = int(dbutils.widgets.get("target_coverage_days"))
STOCK_SECURITY = int(dbutils.widgets.get("stock_security_rollos"))

FULL_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_NAME}"
SILVER_TABLE = f"{FULL_SCHEMA}.silver_inventario_consolidado"
GOLD_TABLE = f"{FULL_SCHEMA}.gold_plan_abastecimiento"

print(f"[GOLD] Silver table: {SILVER_TABLE}")
print(f"[GOLD] Gold table: {GOLD_TABLE}")
print(f"[GOLD] Forecast horizon: {FORECAST_HORIZON} days")
print(f"[GOLD] Target coverage: {TARGET_COVERAGE} days")
print(f"[GOLD] Stock security: {STOCK_SECURITY} rolls")

# COMMAND ----------

# DBTITLE 1,Load Silver table
# Load Silver table
df_silver = spark.table(SILVER_TABLE)
print(f"[GOLD] Silver records loaded: {df_silver.count()}")
df_silver.printSchema()

# COMMAND ----------

# DBTITLE 1,Feature engineering: SALDO_DIAS and RANGO_COBERTURA_T
# Feature engineering for ML model
print("[GOLD] Feature engineering...")

df_features = df_silver.withColumn(
    'SALDO_DIAS', 
    F.when(F.col('PROM_TRANSACCIONES') > 0, 
           F.col('SALDO_ROLLOS') / (F.col('PROM_TRANSACCIONES') / 30.0))
    .otherwise(F.lit(999.0))
).withColumn(
    'RANGO_COBERTURA_T',
    F.when(F.col('SALDO_DIAS') >= 60, F.lit('ALTA'))
    .when(F.col('SALDO_DIAS') >= 30, F.lit('MEDIA'))
    .when(F.col('SALDO_DIAS') >= 7, F.lit('BAJA'))
    .otherwise(F.lit('CRITICA'))
)

print(f"[GOLD] Features created. Records: {df_features.count()}")
df_features.select('cncodpus', 'SALDO_ROLLOS', 'PROM_TRANSACCIONES', 'SALDO_DIAS', 'RANGO_COBERTURA_T').show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Train RandomForestRegressor model
# Prepare ML pipeline
categorical_cols = ['DEPARTAMENTO', 'TIPOLOGIA_OPERACIONES', 'SEDE_ROLLOS']
numerical_cols = ['PROM_TRANSACCIONES', 'PPTO_TRANSP', 'SALDO_ROLLOS', 'ROLLOS_CONSUMIDOS']

# Index and encode categorical features
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid='keep') 
            for col in categorical_cols]
encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_enc") 
            for col in categorical_cols]

# Assemble features
assembler_inputs = [f"{col}_enc" for col in categorical_cols] + numerical_cols
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid='keep')

# RandomForestRegressor per project spec: 100 trees, maxDepth=10
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="ROLLOS_CONSUMIDOS",
    numTrees=100,
    maxDepth=10,
    seed=42
)

ml_pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

print("[GOLD] Training RandomForestRegressor (100 trees, maxDepth=10)...")
ml_model = ml_pipeline.fit(df_features)
print("[GOLD] Model trained successfully")

# COMMAND ----------

# DBTITLE 1,Generate forecasts and calculate lote optimo
# Generate demand forecast for next 30 days
print("[GOLD] Generating 30-day demand forecast...")

df_predictions = ml_model.transform(df_features)

df_gold = df_predictions.withColumn(
    'FORECAST_DEMANDA_30D', F.col('prediction')
).withColumn(
    'LOTE_OPTIMO',
    F.ceil(
        (F.lit(TARGET_COVERAGE) - F.col('SALDO_DIAS')) * F.col('PROM_TRANSACCIONES') / F.lit(30.0) / F.lit(12)
    ) * F.lit(12)
).withColumn(
    'LOTE_OPTIMO',
    F.when(F.col('LOTE_OPTIMO') < 0, F.lit(0)).otherwise(F.col('LOTE_OPTIMO'))
).withColumn(
    'SUGERENCIA_FINAL',
    F.when(F.col('ESTADO_INVENTARIO') == 'DESABASTECIDO', F.lit('DESPACHO INMEDIATO'))
    .when(F.col('LOTE_OPTIMO') > 0, F.lit('PEDIDO PREVENTIVO'))
    .otherwise(F.lit('MONITOREO'))
)

print(f"[GOLD] Predictions generated. Records: {df_gold.count()}")

# COMMAND ----------

# DBTITLE 1,Write Gold table
# Write to Gold table
df_gold_final = df_gold.select(
    'cncodpus', 'SEDE_ROLLOS', 'DEPARTAMENTO', 'MUNICIPIO',
    'TIPOLOGIA_OPERACIONES', 'ESTADO_INVENTARIO', 'SUGERENCIA_ACCION',
    'SALDO_ROLLOS', 'SALDO_DIAS', 'RANGO_COBERTURA_T',
    'PROM_TRANSACCIONES', 'ROLLOS_CONSUMIDOS', 'ROLLOS_REQUERIDOS',
    'FORECAST_DEMANDA_30D', 'LOTE_OPTIMO', 'SUGERENCIA_FINAL',
    'HAS_ACTIVE_OC',
    F.lit(datetime.now()).alias('FECHA_PROCESO'),
    F.lit('1.0').alias('PIPELINE_VERSION')
)

(
    df_gold_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(GOLD_TABLE)
)

print(f"[GOLD] Gold table saved: {GOLD_TABLE}")
print(f"[GOLD] Total records: {spark.table(GOLD_TABLE).count()}")

# COMMAND ----------

# DBTITLE 1,Gold validation summary
# Validation summary
print("=" * 70)
print("[GOLD] RESUMEN DE PLAN DE ABASTECIMIENTO")
print("=" * 70)

df_result = spark.table(GOLD_TABLE)

print(f"\nTotal PUS procesados: {df_result.count()}")

print("\nDistribucion SUGERENCIA_FINAL:")
df_result.groupBy('SUGERENCIA_FINAL').count().orderBy('SUGERENCIA_FINAL').show()

print("\nDistribucion RANGO_COBERTURA_T:")
df_result.groupBy('RANGO_COBERTURA_T').count().orderBy('RANGO_COBERTURA_T').show()

total_lote = df_result.agg(F.sum('LOTE_OPTIMO')).collect()[0][0]
print(f"\nTotal LOTE_OPTIMO (rollos a despachar): {total_lote:,.0f}")

print("\nTop 5 PUS con mayor LOTE_OPTIMO:")
df_result.select('cncodpus', 'DEPARTAMENTO', 'ESTADO_INVENTARIO', 'LOTE_OPTIMO', 'SUGERENCIA_FINAL') \
    .orderBy(F.desc('LOTE_OPTIMO')).limit(5).show(truncate=False)

print("=" * 70)
print("[GOLD] Pipeline Gold completado exitosamente.")
print("=" * 70)

# COMMAND ----------

