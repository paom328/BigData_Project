# Databricks notebook source
# DBTITLE 1,Unity Catalog Setup - Phase 1
# MAGIC %md
# MAGIC # Unity Catalog Setup - Phase 1
# MAGIC ## BigData Project Infrastructure
# MAGIC
# MAGIC **Purpose:** Create the Unity Catalog infrastructure for the Paper Roll Logistics project
# MAGIC
# MAGIC **Components to Create:**
# MAGIC 1. Catalog: `catalog_logistica`
# MAGIC 2. Schema: `db_rollos`
# MAGIC 3. Volume: `landing` (for raw Excel files)
# MAGIC
# MAGIC **Execution Time:** ~2 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ⚠️ **Prerequisites:**
# MAGIC * You must have `CREATE CATALOG` permission
# MAGIC * You must have `CREATE SCHEMA` permission
# MAGIC * You must have `CREATE VOLUME` permission
# MAGIC
# MAGIC If you encounter permission errors, contact your Databricks workspace administrator.

# COMMAND ----------

# DBTITLE 1,Step 1: Create Catalog
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 1: CREATE CATALOG
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_logistica
# MAGIC COMMENT 'Logistics and Supply Chain Data - Paper Roll Distribution Network';
# MAGIC
# MAGIC -- Verify catalog creation
# MAGIC SHOW CATALOGS LIKE 'catalog_logistica';

# COMMAND ----------

# DBTITLE 1,Step 2: Set Current Catalog
# MAGIC %sql
# MAGIC -- Set active catalog
# MAGIC USE CATALOG catalog_logistica;
# MAGIC
# MAGIC SELECT current_catalog() as current_catalog;

# COMMAND ----------

# DBTITLE 1,Step 3: Create Schema
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 3: CREATE SCHEMA
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS db_rollos
# MAGIC COMMENT 'Paper Roll Inventory and Distribution Data - Bronze, Silver, Gold Layers';
# MAGIC
# MAGIC -- Verify schema creation
# MAGIC SHOW SCHEMAS IN catalog_logistica;

# COMMAND ----------

# DBTITLE 1,Step 4: Create Landing Volume
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 4: CREATE LANDING VOLUME
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS catalog_logistica.db_rollos.landing
# MAGIC COMMENT 'Landing zone for raw Excel files (Modelo_bigdata, Stock_Wompi, OC AGOSTO)';
# MAGIC
# MAGIC -- Verify volume creation
# MAGIC SHOW VOLUMES IN catalog_logistica.db_rollos;

# COMMAND ----------

# DBTITLE 1,Step 5: Grant Permissions (Optional)
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 5: GRANT PERMISSIONS (OPTIONAL)
# MAGIC -- ========================================
# MAGIC -- Uncomment and modify these if you need to grant permissions to other users
# MAGIC
# MAGIC -- Grant catalog usage
# MAGIC -- GRANT USE CATALOG ON CATALOG catalog_logistica TO `other.user@example.com`;
# MAGIC
# MAGIC -- Grant schema permissions
# MAGIC -- GRANT USE SCHEMA ON SCHEMA catalog_logistica.db_rollos TO `other.user@example.com`;
# MAGIC -- GRANT CREATE TABLE ON SCHEMA catalog_logistica.db_rollos TO `other.user@example.com`;
# MAGIC
# MAGIC -- Grant volume permissions
# MAGIC -- GRANT READ FILES ON VOLUME catalog_logistica.db_rollos.landing TO `other.user@example.com`;
# MAGIC -- GRANT WRITE FILES ON VOLUME catalog_logistica.db_rollos.landing TO `other.user@example.com`;

# COMMAND ----------

# DBTITLE 1,Step 6: Verify Setup
# ========================================
# STEP 6: VERIFY UNITY CATALOG SETUP
# ========================================

print("="*70)
print("UNITY CATALOG SETUP VERIFICATION")
print("="*70)

# Check catalog
catalogs = spark.sql("SHOW CATALOGS").filter("catalog = 'catalog_logistica'").count()
if catalogs > 0:
    print("\n✅ Catalog 'catalog_logistica' exists")
else:
    print("\n❌ Catalog 'catalog_logistica' NOT found")

# Check schema
schemas = spark.sql("SHOW SCHEMAS IN catalog_logistica").filter("databaseName = 'db_rollos'").count()
if schemas > 0:
    print("✅ Schema 'db_rollos' exists")
else:
    print("❌ Schema 'db_rollos' NOT found")

# Check volume
volumes = spark.sql("SHOW VOLUMES IN catalog_logistica.db_rollos").filter("volume_name = 'landing'").count()
if volumes > 0:
    print("✅ Volume 'landing' exists")
else:
    print("❌ Volume 'landing' NOT found")

# Get volume path
print("\n" + "-"*70)
print("Volume Path Information:")
print("-"*70)
volume_info = spark.sql("DESCRIBE VOLUME catalog_logistica.db_rollos.landing").collect()
for row in volume_info:
    if row.info_name == "Storage Location":
        print(f"  📍 Storage Location: {row.info_value}")

print("\n" + "-"*70)
print("💾 Upload Path for Excel Files:")
print("-"*70)
print("  /Volumes/catalog_logistica/db_rollos/landing/")
print("\n  Upload these files:")
print("    1. Modelo_bigdata.xlsx")
print("    2. Stock_Wompi_Rollos_2026-09-12.xlsx")
print("    3. OC AGOSTO.xlsx")

# COMMAND ----------

# DBTITLE 1,Setup Complete - Next Steps
# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC
# MAGIC Your Unity Catalog infrastructure is ready.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📤 Next Step: Upload Excel Files
# MAGIC
# MAGIC ### Option 1: Via Databricks UI
# MAGIC 1. Navigate to **Catalog** in the left sidebar
# MAGIC 2. Expand `catalog_logistica` → `db_rollos` → `landing`
# MAGIC 3. Click **Upload Files**
# MAGIC 4. Upload all three Excel files:
# MAGIC    * `Modelo_bigdata.xlsx`
# MAGIC    * `Stock_Wompi_Rollos_2026-09-12.xlsx`
# MAGIC    * `OC AGOSTO.xlsx`
# MAGIC
# MAGIC ### Option 2: Via Databricks CLI
# MAGIC ```bash
# MAGIC databricks fs cp Modelo_bigdata.xlsx \
# MAGIC   dbfs:/Volumes/catalog_logistica/db_rollos/landing/
# MAGIC
# MAGIC databricks fs cp Stock_Wompi_Rollos_2026-09-12.xlsx \
# MAGIC   dbfs:/Volumes/catalog_logistica/db_rollos/landing/
# MAGIC
# MAGIC databricks fs cp "OC AGOSTO.xlsx" \
# MAGIC   dbfs:/Volumes/catalog_logistica/db_rollos/landing/
# MAGIC ```
# MAGIC
# MAGIC ### Option 3: Via Python (dbutils)
# MAGIC ```python
# MAGIC # Copy from local DBFS to Volume
# MAGIC dbutils.fs.cp(
# MAGIC     "file:/Workspace/path/to/Modelo_bigdata.xlsx",
# MAGIC     "/Volumes/catalog_logistica/db_rollos/landing/Modelo_bigdata.xlsx"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 After Upload: Run Bronze Ingestion
# MAGIC
# MAGIC Once files are uploaded, execute:
# MAGIC **📓 01_Bronze_Ingestion** notebook
# MAGIC
# MAGIC This will load the Excel files into Bronze Delta tables.

# COMMAND ----------

