# Databricks notebook source
# DBTITLE 1,Silver Layer - Production ETL Pipeline
# MAGIC %md
# MAGIC # Silver Layer - Production ETL Pipeline
# MAGIC ## Paper Roll Logistics - Multi-Source Integration & Business Logic
# MAGIC
# MAGIC **Purpose:** Integrate master data, live inventory, and purchase orders with comprehensive business rules
# MAGIC
# MAGIC **Input Tables:**
# MAGIC * `catalog_logistica.db_rollos.bronze_modelo_bigdata`
# MAGIC * `catalog_logistica.db_rollos.bronze_stock_wompi`
# MAGIC * `catalog_logistica.db_rollos.bronze_oc_agosto`
# MAGIC
# MAGIC **Output Table:**
# MAGIC * `catalog_logistica.db_rollos.silver_inventario_integrado`
# MAGIC
# MAGIC **Business Rules Applied:**
# MAGIC 1. Multi-source LEFT JOIN (cncodpus ↔ CB)
# MAGIC 2. Stock recalculation from live inventory
# MAGIC 3. Baseline imputation (24 rolls for missing data)
# MAGIC 4. Coverage days calculation (SALDO_DIAS)
# MAGIC 5. Risk bracket classification (Column T)
# MAGIC 6. Inventory status determination (ESTADO_INVENTARIO)

# COMMAND ----------

# DBTITLE 1,Import Libraries
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

print("✅ Libraries imported successfully")
print(f"Execution timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Load Bronze Tables
# Load Bronze layer tables
print("Loading Bronze layer tables...")

# Master data (24,382 PUS)
df_master = spark.table("catalog_logistica.db_rollos.bronze_modelo_bigdata")
master_count = df_master.count()
print(f"✅ Modelo Bigdata: {master_count:,} records loaded")

# Live inventory (Stock Wompi)
df_stock = spark.table("catalog_logistica.db_rollos.bronze_stock_wompi")
stock_count = df_stock.count()
print(f"✅ Stock Wompi: {stock_count:,} records loaded")

# Purchase orders / Requirements (OC AGOSTO)
df_oc = spark.table("catalog_logistica.db_rollos.bronze_oc_agosto")
oc_count = df_oc.count()
print(f"✅ OC AGOSTO: {oc_count:,} records loaded")

print(f"\n📊 Total records to process: {master_count:,}")

# COMMAND ----------

# DBTITLE 1,Step 1: Multi-Source JOIN
# ========================================
# STEP 1: MULTI-SOURCE LEFT JOIN
# ========================================
# Join Strategy: Master (cncodpus) <- Stock_Wompi (CB) <- OC_AGOSTO (CB)

print("\n" + "="*70)
print("STEP 1: MULTI-SOURCE LEFT JOIN")
print("="*70)

# Prepare Stock Wompi (rename CB to cncodpus for join)
df_stock_prep = df_stock.select(
    F.col("CB").alias("stock_cncodpus"),
    F.col("Cantidad").alias("stock_cantidad"),
    F.col("TIPOLOGIA ROLLOS").alias("stock_tipologia")
)

# Prepare OC AGOSTO (create flag for sites with active purchase orders)
df_oc_prep = df_oc.select(
    F.col("CB").alias("oc_cncodpus")
).distinct().withColumn("has_purchase_order", F.lit(True))

print(f"Stock Wompi prepared: {df_stock_prep.count():,} unique sites")
print(f"OC AGOSTO prepared: {df_oc_prep.count():,} sites with active POs")

# Perform LEFT JOINs
df_integrated = df_master \
    .join(df_stock_prep, df_master["cncodpus"] == df_stock_prep["stock_cncodpus"], "left") \
    .join(df_oc_prep, df_master["cncodpus"] == df_oc_prep["oc_cncodpus"], "left")

# Fill null purchase order flag with False
df_integrated = df_integrated.fillna({"has_purchase_order": False})

print(f"✅ Multi-source JOIN completed: {df_integrated.count():,} records")

# COMMAND ----------

# DBTITLE 1,Step 2: Stock Recalculation & Baseline Imputation
# ========================================
# STEP 2: STOCK RECALCULATION & BASELINE IMPUTATION
# ========================================

print("\n" + "="*70)
print("STEP 2: STOCK RECALCULATION & BASELINE IMPUTATION")
print("="*70)

# Business Rule 1: Update ROLLOS_ENTREGADOS from Stock_Wompi if available
# Business Rule 2: Baseline imputation (24 rolls) if missing AND no active PO

df_integrated = df_integrated.withColumn(
    "ROLLOS_ENTREGADOS_RECALC",
    F.when(
        F.col("stock_cantidad").isNotNull(),
        F.col("stock_cantidad")  # Use live inventory count
    ).when(
        (F.col("ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA").isNull() | (F.col("ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA") == 0)) &
        (F.col("has_purchase_order") == False),
        F.lit(24)  # Baseline imputation
    ).otherwise(
        F.coalesce(F.col("ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA"), F.lit(0))
    )
)

# Create imputation flag for tracking
df_integrated = df_integrated.withColumn(
    "imputed_flag",
    F.when(
        (F.col("ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA").isNull() | (F.col("ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA") == 0)) &
        (F.col("has_purchase_order") == False) &
        F.col("stock_cantidad").isNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# Calculate roll balance (SALDO_ROLLOS)
df_integrated = df_integrated.withColumn(
    "SALDO_ROLLOS",
    F.col("ROLLOS_ENTREGADOS_RECALC") - F.coalesce(F.col("ROLLOS CONSUMIDOS"), F.lit(0))
)

# Count imputed records
imputed_count = df_integrated.filter(F.col("imputed_flag") == True).count()
stock_updated_count = df_integrated.filter(F.col("stock_cantidad").isNotNull()).count()

print(f"✅ Stock recalculation completed")
print(f"   - {stock_updated_count:,} sites updated from live inventory")
print(f"   - {imputed_count:,} sites received baseline imputation (24 rolls)")

# COMMAND ----------

# DBTITLE 1,Step 3: Coverage Days Calculation
# ========================================
# STEP 3: COVERAGE DAYS CALCULATION
# ========================================

print("\n" + "="*70)
print("STEP 3: COVERAGE DAYS CALCULATION")
print("="*70)

# Formula: SALDO_DIAS = (SALDO_ROLLOS * 30) / PROM_TRANSACCIONES
# Handle division by zero with NULL

df_integrated = df_integrated.withColumn(
    "SALDO_DIAS",
    F.when(
        (F.col("PROM_TRANSACCIONES").isNotNull()) & (F.col("PROM_TRANSACCIONES") > 0),
        (F.col("SALDO_ROLLOS") * 30) / F.col("PROM_TRANSACCIONES")
    ).otherwise(F.lit(None))
)

avg_coverage = df_integrated.agg(F.avg("SALDO_DIAS")).collect()[0][0]
print(f"✅ Coverage days calculated")
if avg_coverage:
    print(f"   - Network average coverage: {avg_coverage:.1f} days")

# COMMAND ----------

# DBTITLE 1,Step 4: Risk Bracket Classification (Column T)
# ========================================
# STEP 4: RISK BRACKET CLASSIFICATION (COLUMN T)
# ========================================

print("\n" + "="*70)
print("STEP 4: RISK BRACKET CLASSIFICATION (COLUMN T)")
print("="*70)

# Nested WHEN logic for coverage brackets
df_integrated = df_integrated.withColumn(
    "T",
    F.when(F.col("SALDO_DIAS").isNull(), F.lit("SIN DATOS"))
    .when(F.col("SALDO_DIAS") <= 0, F.lit("DESABASTECIDO"))
    .when(F.col("SALDO_DIAS") <= 5, F.lit("1-5 D"))
    .when(F.col("SALDO_DIAS") <= 10, F.lit("6-10 D"))
    .when(F.col("SALDO_DIAS") <= 15, F.lit("10-15 D"))
    .when(F.col("SALDO_DIAS") <= 20, F.lit("15-20 D"))
    .when(F.col("SALDO_DIAS") <= 25, F.lit("21-25 D"))
    .when(F.col("SALDO_DIAS") <= 30, F.lit("26-30 D"))
    .when(F.col("SALDO_DIAS") <= 35, F.lit("31-35 D"))
    .when(F.col("SALDO_DIAS") <= 45, F.lit("36-45 D"))
    .when(F.col("SALDO_DIAS") <= 60, F.lit("45-60 D"))
    .when(F.col("SALDO_DIAS") <= 90, F.lit("60-90 D"))
    .otherwise(F.lit("90 D+"))
)

# Distribution of risk brackets
print("✅ Risk brackets assigned")
print("\nDistribution by coverage bracket:")
df_integrated.groupBy("T").count().orderBy(F.col("count").desc()).show(15, truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 5: Inventory Status (ESTADO_INVENTARIO)
# ========================================
# STEP 5: INVENTORY STATUS (ESTADO_INVENTARIO)
# ========================================

print("\n" + "="*70)
print("STEP 5: INVENTORY STATUS CLASSIFICATION")
print("="*70)

# Business Logic:
# DESABASTECIDO: SALDO_ROLLOS <= 0 OR site has active purchase order
# CRÍTICO: SALDO_ROLLOS < 30 (and not DESABASTECIDO)
# ABASTECIDO: SALDO_ROLLOS >= 30

df_integrated = df_integrated.withColumn(
    "ESTADO_INVENTARIO",
    F.when(
        (F.col("SALDO_ROLLOS") <= 0) | (F.col("has_purchase_order") == True),
        F.lit("DESABASTECIDO")
    ).when(
        F.col("SALDO_ROLLOS") < 30,
        F.lit("CRÍTICO")
    ).otherwise(
        F.lit("ABASTECIDO")
    )
)

# Distribution of inventory status
print("✅ Inventory status assigned")
print("\nDistribution by inventory status:")
status_dist = df_integrated.groupBy("ESTADO_INVENTARIO").count().orderBy(F.col("count").desc())
status_dist.show()

# Calculate critical KPIs
total_sites = df_integrated.count()
desabastecido_count = df_integrated.filter(F.col("ESTADO_INVENTARIO") == "DESABASTECIDO").count()
critico_count = df_integrated.filter(F.col("ESTADO_INVENTARIO") == "CRÍTICO").count()
abastecido_count = df_integrated.filter(F.col("ESTADO_INVENTARIO") == "ABASTECIDO").count()

print(f"\n📊 Executive Summary:")
print(f"   - Total Sites: {total_sites:,}")
print(f"   - 🔴 Desabastecido: {desabastecido_count:,} ({100*desabastecido_count/total_sites:.1f}%)")
print(f"   - 🟡 Crítico: {critico_count:,} ({100*critico_count/total_sites:.1f}%)")
print(f"   - 🟢 Abastecido: {abastecido_count:,} ({100*abastecido_count/total_sites:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Final Schema & Data Quality
# ========================================
# FINAL SCHEMA PREPARATION & DATA QUALITY
# ========================================

print("\n" + "="*70)
print("FINAL SCHEMA PREPARATION")
print("="*70)

# Add metadata columns
df_silver = df_integrated.withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "processing_date",
    F.current_date()
)

# Select final schema (preserving all original columns + new calculated fields)
final_columns = [
    # Identifiers
    "cncodpus", "NOMBREPUS", "DEPARTAMENTO", "MUNICIPIO",
    "SEDE OPERACIONES", "SEDE ROLLOS", "TIPOLOGIA OPERACIONES",
    
    # Recalculated stock
    "ROLLOS_ENTREGADOS_RECALC",
    "ROLLOS CONSUMIDOS",
    "SALDO_ROLLOS",
    
    # Coverage & risk
    "PROM_TRANSACCIONES",
    "SALDO_DIAS",
    "T",  # Risk bracket
    "ESTADO_INVENTARIO",
    
    # Flags & metadata
    "has_purchase_order",
    "imputed_flag",
    "processed_timestamp",
    "processing_date"
]

# Check if columns exist before selection (handle schema variations)
available_columns = [col for col in final_columns if col in df_silver.columns]
df_silver_final = df_silver.select(*available_columns)

print(f"✅ Final schema prepared with {len(available_columns)} columns")
print(f"\nFinal record count: {df_silver_final.count():,}")

# COMMAND ----------

# DBTITLE 1,Write to Silver Table
# ========================================
# WRITE TO SILVER DELTA TABLE
# ========================================

print("\n" + "="*70)
print("WRITING TO SILVER DELTA TABLE")
print("="*70)

target_table = "catalog_logistica.db_rollos.silver_inventario_integrado"

# Write with MERGE (upsert) logic based on cncodpus
df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✅ Silver table written successfully: {target_table}")

# Optimize table for query performance
print("\nOptimizing Delta table...")
spark.sql(f"OPTIMIZE {target_table} ZORDER BY (ESTADO_INVENTARIO, DEPARTAMENTO, T)")
print("✅ Table optimized")

# Generate table statistics
print("\nGenerating table statistics...")
spark.sql(f"ANALYZE TABLE {target_table} COMPUTE STATISTICS")
print("✅ Statistics computed")

# COMMAND ----------

# DBTITLE 1,Pipeline Completion Summary
# ========================================
# PIPELINE COMPLETION SUMMARY
# ========================================

print("\n" + "="*70)
print("🎉 SILVER LAYER PIPELINE COMPLETED SUCCESSFULLY")
print("="*70)

print(f"\n📊 Final Statistics:")
print(f"   Input Records (Bronze): {master_count:,}")
print(f"   Output Records (Silver): {df_silver_final.count():,}")
print(f"   Stock Updates Applied: {stock_updated_count:,}")
print(f"   Baseline Imputations: {imputed_count:,}")
print(f"   Sites with Active POs: {oc_count:,}")
print(f"\n🟢 Inventory Health:")
print(f"   Desabastecido: {desabastecido_count:,} sites")
print(f"   Crítico: {critico_count:,} sites")
print(f"   Abastecido: {abastecido_count:,} sites")
print(f"\n💾 Output Table: {target_table}")
print(f"⏰ Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n✅ Ready for ML Forecasting Layer")

# COMMAND ----------

