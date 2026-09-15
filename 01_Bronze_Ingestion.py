# Databricks notebook source
# DBTITLE 1,Bronze Layer - Data Ingestion Pipeline
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion Pipeline
# MAGIC ## Paper Roll Logistics - Excel to Delta Lake
# MAGIC
# MAGIC **Purpose:** Load raw Excel files from Unity Catalog Volume into Bronze Delta tables
# MAGIC
# MAGIC **Source Location:** `/Volumes/catalog_logistica/db_rollos/landing/`
# MAGIC
# MAGIC **Target Tables:**
# MAGIC * `catalog_logistica.db_rollos.bronze_modelo_bigdata`
# MAGIC * `catalog_logistica.db_rollos.bronze_stock_wompi`
# MAGIC * `catalog_logistica.db_rollos.bronze_oc_agosto`
# MAGIC
# MAGIC **Execution Time:** ~10 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Sources
# MAGIC
# MAGIC 1. **Modelo_bigdata.xlsx** - Master data (24,382 PUS)
# MAGIC 2. **Stock_Wompi_Rollos_2026-09-12.xlsx** - Live inventory
# MAGIC 3. **OC AGOSTO.xlsx** - Purchase orders / Requirements

# COMMAND ----------

# DBTITLE 1,Configuration & Setup
# Import required libraries
from pyspark.sql import functions as F
from datetime import datetime

# Configuration
LANDING_VOLUME = "/Volumes/catalog_logistica/db_rollos/landing"
CATALOG = "catalog_logistica"
SCHEMA = "db_rollos"

print("✅ Configuration loaded")
print(f"Landing Volume: {LANDING_VOLUME}")
print(f"Target Catalog: {CATALOG}")
print(f"Target Schema: {SCHEMA}")
print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Verify Landing Volume Files
# Check if files exist in landing volume
print("\n" + "="*70)
print("VERIFYING LANDING VOLUME FILES")
print("="*70)

try:
    files = dbutils.fs.ls(LANDING_VOLUME)
    
    print(f"\n📁 Files found in {LANDING_VOLUME}:")
    print("-" * 70)
    
    for file in files:
        size_mb = file.size / (1024 * 1024)
        print(f"  • {file.name:50s} {size_mb:>10.2f} MB")
    
    # Check for required files
    file_names = [f.name for f in files]
    required_files = [
        "Modelo_bigdata.xlsx",
        "Stock_Wompi_Rollos_2026-09-12.xlsx",
        "OC AGOSTO.xlsx"
    ]
    
    missing_files = [f for f in required_files if f not in file_names]
    
    if missing_files:
        print(f"\n❌ Missing files: {', '.join(missing_files)}")
        print("\n⚠️  Please upload missing files before proceeding.")
    else:
        print("\n✅ All required files found!")
        
except Exception as e:
    print(f"❌ Error accessing landing volume: {str(e)}")
    print("\n⚠️  Ensure Unity Catalog volume exists and you have permissions.")

# COMMAND ----------

# DBTITLE 1,Load Modelo Bigdata (Master Data)
# ========================================
# LOAD 1: MODELO BIGDATA (MASTER DATA)
# ========================================

print("\n" + "="*70)
print("LOADING: Modelo_bigdata.xlsx (Master Data)")
print("="*70)

try:
    # Read Excel file
    df_modelo = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("treatEmptyValuesAsNulls", "true") \
        .option("addColorColumns", "false") \
        .load(f"{LANDING_VOLUME}/Modelo_bigdata.xlsx")
    
    # Add ingestion metadata
    df_modelo = df_modelo.withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("source_file", F.lit("Modelo_bigdata.xlsx"))
    
    # Show schema and sample
    print("\n📋 Schema:")
    df_modelo.printSchema()
    
    record_count = df_modelo.count()
    print(f"\n📊 Record Count: {record_count:,}")
    
    print("\n📄 Sample (first 3 rows):")
    df_modelo.show(3, truncate=False, vertical=True)
    
    # Write to Bronze Delta table
    target_table = f"{CATALOG}.{SCHEMA}.bronze_modelo_bigdata"
    
    df_modelo.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"\n✅ Bronze table created: {target_table}")
    print(f"   Records written: {record_count:,}")
    
except Exception as e:
    print(f"\n❌ Error loading Modelo_bigdata: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Load Stock Wompi (Live Inventory)
# ========================================
# LOAD 2: STOCK WOMPI (LIVE INVENTORY)
# ========================================

print("\n" + "="*70)
print("LOADING: Stock_Wompi_Rollos_2026-09-12.xlsx (Live Inventory)")
print("="*70)

try:
    # Read Excel file
    df_stock = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("treatEmptyValuesAsNulls", "true") \
        .option("addColorColumns", "false") \
        .load(f"{LANDING_VOLUME}/Stock_Wompi_Rollos_2026-09-12.xlsx")
    
    # Add ingestion metadata
    df_stock = df_stock.withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("source_file", F.lit("Stock_Wompi_Rollos_2026-09-12.xlsx"))
    
    # Show schema and sample
    print("\n📋 Schema:")
    df_stock.printSchema()
    
    record_count = df_stock.count()
    print(f"\n📊 Record Count: {record_count:,}")
    
    print("\n📄 Sample (first 3 rows):")
    df_stock.show(3, truncate=False, vertical=True)
    
    # Write to Bronze Delta table
    target_table = f"{CATALOG}.{SCHEMA}.bronze_stock_wompi"
    
    df_stock.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"\n✅ Bronze table created: {target_table}")
    print(f"   Records written: {record_count:,}")
    
except Exception as e:
    print(f"\n❌ Error loading Stock_Wompi: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Load OC AGOSTO (Purchase Orders)
# ========================================
# LOAD 3: OC AGOSTO (PURCHASE ORDERS)
# ========================================

print("\n" + "="*70)
print("LOADING: OC AGOSTO.xlsx (Purchase Orders / Requirements)")
print("="*70)

try:
    # Read Excel file
    df_oc = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("treatEmptyValuesAsNulls", "true") \
        .option("addColorColumns", "false") \
        .load(f"{LANDING_VOLUME}/OC AGOSTO.xlsx")
    
    # Add ingestion metadata
    df_oc = df_oc.withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("source_file", F.lit("OC AGOSTO.xlsx"))
    
    # Show schema and sample
    print("\n📋 Schema:")
    df_oc.printSchema()
    
    record_count = df_oc.count()
    print(f"\n📊 Record Count: {record_count:,}")
    
    print("\n📄 Sample (first 3 rows):")
    df_oc.show(3, truncate=False, vertical=True)
    
    # Write to Bronze Delta table
    target_table = f"{CATALOG}.{SCHEMA}.bronze_oc_agosto"
    
    df_oc.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"\n✅ Bronze table created: {target_table}")
    print(f"   Records written: {record_count:,}")
    
except Exception as e:
    print(f"\n❌ Error loading OC AGOSTO: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Data Quality Summary
# ========================================
# DATA QUALITY SUMMARY
# ========================================

print("\n" + "="*70)
print("DATA QUALITY SUMMARY")
print("="*70)

# Load all Bronze tables
df_modelo_check = spark.table(f"{CATALOG}.{SCHEMA}.bronze_modelo_bigdata")
df_stock_check = spark.table(f"{CATALOG}.{SCHEMA}.bronze_stock_wompi")
df_oc_check = spark.table(f"{CATALOG}.{SCHEMA}.bronze_oc_agosto")

print("\n📊 Record Counts:")
print("-" * 70)
print(f"  • Modelo Bigdata (Master):  {df_modelo_check.count():>10,} records")
print(f"  • Stock Wompi (Inventory):  {df_stock_check.count():>10,} records")
print(f"  • OC AGOSTO (Orders):       {df_oc_check.count():>10,} records")

# Check for nulls in key columns
print("\n🔍 Data Quality Checks:")
print("-" * 70)

# Modelo Bigdata checks
modelo_nulls = df_modelo_check.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["cncodpus", "NOMBREPUS", "DEPARTAMENTO"]]
).collect()[0].asDict()

print("\n  Modelo Bigdata (Master Data):")
for col, null_count in modelo_nulls.items():
    status = "✅" if null_count == 0 else "⚠️"
    print(f"    {status} {col}: {null_count:,} nulls")

# Stock Wompi checks
if "CB" in df_stock_check.columns and "Cantidad" in df_stock_check.columns:
    stock_nulls = df_stock_check.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["CB", "Cantidad"]]
    ).collect()[0].asDict()
    
    print("\n  Stock Wompi (Live Inventory):")
    for col, null_count in stock_nulls.items():
        status = "✅" if null_count == 0 else "⚠️"
        print(f"    {status} {col}: {null_count:,} nulls")

# OC AGOSTO checks
if "CB" in df_oc_check.columns:
    oc_nulls = df_oc_check.select(
        F.sum(F.col("CB").isNull().cast("int")).alias("CB")
    ).collect()[0]["CB"]
    
    print("\n  OC AGOSTO (Purchase Orders):")
    status = "✅" if oc_nulls == 0 else "⚠️"
    print(f"    {status} CB: {oc_nulls:,} nulls")

# COMMAND ----------

# DBTITLE 1,Bronze Layer Completion
# ========================================
# BRONZE LAYER COMPLETION SUMMARY
# ========================================

print("\n" + "="*70)
print("🎉 BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
print("="*70)

print("\n✅ Tables Created:")
print("-" * 70)
print(f"  1. {CATALOG}.{SCHEMA}.bronze_modelo_bigdata")
print(f"  2. {CATALOG}.{SCHEMA}.bronze_stock_wompi")
print(f"  3. {CATALOG}.{SCHEMA}.bronze_oc_agosto")

print("\n📍 Next Step:")
print("-" * 70)
print("  Execute the Silver Transformation notebook:")
print("  👉 02_Silver_Transformation_Production")
print("\n  This will apply business logic:")
print("     • Multi-source JOIN (cncodpus ↔ CB)")
print("     • Stock recalculation from live inventory")
print("     • Baseline imputation (24 rolls)")
print("     • Coverage days calculation")
print("     • Risk bracket classification")
print("     • Inventory status determination")

print(f"\n⏰ Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# COMMAND ----------

