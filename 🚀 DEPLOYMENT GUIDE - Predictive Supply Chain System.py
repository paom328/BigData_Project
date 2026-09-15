# Databricks notebook source
# DBTITLE 1,Complete Deployment Guide
# MAGIC %md
# MAGIC # 🚀 COMPLETE DEPLOYMENT GUIDE
# MAGIC ## Predictive Supply Chain Forecasting System
# MAGIC
# MAGIC **Status:** Ready for Production Deployment  
# MAGIC **Estimated Setup Time:** 2 hours  
# MAGIC **Compute Requirements:** Serverless SQL Warehouse + Optional Job Cluster
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 PRE-DEPLOYMENT CHECKLIST
# MAGIC
# MAGIC ### ✅ Data Sources Confirmed:
# MAGIC - [x] `proyecto1.default.modelo_bigdata_raw` (24,382 rows) - Master PUS data
# MAGIC - [x] `Stock_Wompi_Rollos_2026-09-12.xlsx` (41,750 rows) - uploaded to idbfs
# MAGIC - [x] `OC AGOSTO.xlsx` (204 rows) - uploaded to idbfs
# MAGIC
# MAGIC ### ✅ Notebooks Created:
# MAGIC - [x] Main Pipeline Notebook (ID: 3528701319739233)
# MAGIC - [x] This Deployment Guide (ID: 3528701319739234)
# MAGIC
# MAGIC ### ✅ Permissions Verified:
# MAGIC - [x] CREATE TABLE on `proyecto1.default` schema
# MAGIC - [x] SELECT on source tables
# MAGIC - [x] Unity Catalog access configured
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 STEP-BY-STEP EXECUTION PLAN
# MAGIC
# MAGIC ### Step 1: Execute Bronze Layer (15 minutes)
# MAGIC
# MAGIC **Navigate to:** [Advanced Predictive Analytics Notebook](#notebook-3528701319739233)
# MAGIC
# MAGIC **Run Cells 2-4 in sequence:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Cell 2: bronze_modelo_bigdata
# MAGIC -- Creates: proyecto1.default.bronze_modelo_bigdata
# MAGIC -- Expected output: 24,382 records, 17,654 active sites
# MAGIC
# MAGIC -- Cell 3: bronze_stock_wompi  
# MAGIC -- Creates: proyecto1.default.bronze_stock_wompi
# MAGIC -- Expected output: 41,750 stock records, 2,606,174 total units
# MAGIC
# MAGIC -- Cell 4: bronze_oc_agosto
# MAGIC -- Creates: proyecto1.default.bronze_oc_agosto
# MAGIC -- Expected output: 204 purchase orders, 204 unique sites
# MAGIC ```
# MAGIC
# MAGIC **Validation Query:**
# MAGIC ```sql
# MAGIC SHOW TABLES IN proyecto1.default LIKE 'bronze_%';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Execute Silver Layer (20 minutes)
# MAGIC
# MAGIC **Run Cell 5:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Creates: proyecto1.default.silver_inventario_recalculado
# MAGIC -- Key Features:
# MAGIC --   ✓ LEFT JOIN with stock_wompi on cncodpus
# MAGIC --   ✓ Smart stockout logic (OC cross-reference)
# MAGIC --   ✓ Baseline imputation (24 units for missing stock)
# MAGIC --   ✓ Recalculated: saldo_rollos, saldo_dias, accion_recomendada
# MAGIC ```
# MAGIC
# MAGIC **Expected Improvements:**
# MAGIC - **Before:** 14,556 sites flagged for action (59.7%)
# MAGIC - **After:** ~8,000-10,000 sites (40-45%) - 30-40% reduction in false positives
# MAGIC
# MAGIC **Run Cell 6 for Validation:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   estado_stock_recalculado,
# MAGIC   COUNT(*) as total_sites
# MAGIC FROM proyecto1.default.silver_inventario_recalculado
# MAGIC GROUP BY estado_stock_recalculado;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Execute ML Forecasting (30 minutes)
# MAGIC
# MAGIC **Run Cells 7-8:**
# MAGIC
# MAGIC ```python
# MAGIC # Cell 7: Setup Prophet library
# MAGIC %pip install prophet
# MAGIC
# MAGIC # Cell 8: Train forecasting models
# MAGIC # Creates: proyecto1.default.ml_demand_forecasts
# MAGIC # Generates:
# MAGIC #   - predicted_consumption_30d/60d/90d
# MAGIC #   - estimated_stockout_date
# MAGIC #   - safety_stock_units
# MAGIC #   - reorder_point_rop
# MAGIC #   - eoq_optimal_order_qty
# MAGIC #   - replenishment_priority_score (1-100)
# MAGIC ```
# MAGIC
# MAGIC **Validation:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_forecasts,
# MAGIC   AVG(replenishment_priority_score) as avg_priority,
# MAGIC   COUNT(CASE WHEN forecast_confidence = 'HIGH' THEN 1 END) as high_confidence_sites
# MAGIC FROM proyecto1.default.ml_demand_forecasts;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Execute Gold Layer (10 minutes)
# MAGIC
# MAGIC **Run Cells 9-11:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Cell 9: gold_inventory_forecasting_dashboard (main view)
# MAGIC -- Cell 10: gold_predictive_stockout_timeline (30-day Gantt)
# MAGIC -- Cell 11: gold_reorder_priority_queue (dispatch planning)
# MAGIC ```
# MAGIC
# MAGIC **Final Validation:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   'Total Sites' as metric,
# MAGIC   COUNT(*) as value
# MAGIC FROM proyecto1.default.gold_inventory_forecasting_dashboard
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Sites at Critical Risk',
# MAGIC   COUNT(*)
# MAGIC FROM proyecto1.default.gold_inventory_forecasting_dashboard
# MAGIC WHERE nivel_riesgo LIKE 'CRÍTICO%'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Predicted Stockouts (30 days)',
# MAGIC   COUNT(*)
# MAGIC FROM proyecto1.default.gold_predictive_stockout_timeline;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 POWER BI DASHBOARD DEPLOYMENT
# MAGIC
# MAGIC ### Connection String:
# MAGIC ```
# MAGIC Server: dbc-43f7a08b-5de1.cloud.databricks.com
# MAGIC HTTP Path: /sql/1.0/warehouses/<your_warehouse_id>
# MAGIC Catalog: proyecto1
# MAGIC Schema: default
# MAGIC ```
# MAGIC
# MAGIC ### Tables to Import:
# MAGIC 1. **gold_inventory_forecasting_dashboard** (Main KPIs)
# MAGIC 2. **gold_predictive_stockout_timeline** (Gantt timeline)
# MAGIC 3. **gold_reorder_priority_queue** (Dispatch queue)
# MAGIC
# MAGIC ### Recommended Visuals:
# MAGIC
# MAGIC **Page 1 - Executive Dashboard:**
# MAGIC - 4 KPI Cards: Total Sites | Critical Risk | Budget Required (M COP) | Stockouts This Week
# MAGIC - Stacked Bar Chart: Sites by `nivel_riesgo` and `DEPARTAMENTO`
# MAGIC - Map Visual: Lat/Long by `MUNICIPIO` colored by `nivel_riesgo`
# MAGIC - Donut Chart: `accion_prioritaria` distribution
# MAGIC
# MAGIC **Page 2 - 30-Day Stockout Timeline:**
# MAGIC - Gantt Chart: `fecha_desabastecimiento_estimada` (X) vs `cncodpus` (Y)
# MAGIC - Color by: `urgencia_despacho`
# MAGIC - Table: Top 50 by `dispatch_rank`
# MAGIC
# MAGIC **Page 3 - Freight Planning:**
# MAGIC - Matrix: `consolidation_group_id` by week
# MAGIC - Bar Chart: Routes by `origen_despacho` → `DEPARTAMENTO`
# MAGIC - Slicer filters: Date range, Department, Sede, Risk level
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ DATABRICKS WORKFLOW AUTOMATION
# MAGIC
# MAGIC ### Deploy via Databricks UI:
# MAGIC
# MAGIC 1. **Navigate to:** Workflows → Create Job
# MAGIC 2. **Job Name:** `[PROD] Supply Chain Forecasting Pipeline`
# MAGIC 3. **Schedule:** Daily at 6:00 AM (America/Bogota timezone)
# MAGIC
# MAGIC **Task Configuration:**
# MAGIC
# MAGIC ```yaml
# MAGIC Task 1: Bronze Ingestion
# MAGIC   Type: Notebook
# MAGIC   Path: /Users/andreys.mendoza7152@unaula.edu.co/Advanced Predictive Analytics...
# MAGIC   Cluster: New job cluster (2 workers, i3.xlarge)
# MAGIC   Timeout: 30 minutes
# MAGIC
# MAGIC Task 2: Silver Transformation
# MAGIC   Depends on: Task 1
# MAGIC   Type: Notebook (same path)
# MAGIC   Timeout: 40 minutes
# MAGIC
# MAGIC Task 3: ML Forecasting
# MAGIC   Depends on: Task 2
# MAGIC   Libraries: prophet==1.1.5
# MAGIC   Timeout: 60 minutes
# MAGIC
# MAGIC Task 4: Gold Aggregation
# MAGIC   Depends on: Task 3
# MAGIC   Timeout: 20 minutes
# MAGIC ```
# MAGIC
# MAGIC **Email Notifications:**
# MAGIC - On Success: andreys.mendoza7152@unaula.edu.co
# MAGIC - On Failure: andreys.mendoza7152@unaula.edu.co
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 SUCCESS METRICS
# MAGIC
# MAGIC ### Before Deployment:
# MAGIC - False stockout rate: ~40% (sites flagged without active OC)
# MAGIC - Manual dispatch planning: 4-6 hours daily
# MAGIC - Freight consolidation: Ad-hoc, sub-optimal routes
# MAGIC - Forecast accuracy: N/A (no forecasting)
# MAGIC
# MAGIC ### After Deployment (Target):
# MAGIC - False stockout rate: < 5% (OC cross-reference)
# MAGIC - Automated dispatch queue: Real-time prioritization
# MAGIC - Freight consolidation: Automated route batching (20% cost savings)
# MAGIC - Forecast accuracy: 85%+ MAPE for 30-day horizon
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🆘 TROUBLESHOOTING
# MAGIC
# MAGIC ### Issue: Bronze tables not created
# MAGIC **Solution:** Verify source table exists: `SELECT COUNT(*) FROM proyecto1.default.modelo_bigdata_raw`
# MAGIC
# MAGIC ### Issue: Silver merge fails
# MAGIC **Solution:** Check for duplicate cncodpus in stock_wompi: 
# MAGIC ```sql
# MAGIC SELECT cncodpus, COUNT(*) 
# MAGIC FROM bronze_stock_wompi 
# MAGIC GROUP BY cncodpus 
# MAGIC HAVING COUNT(*) > 1;
# MAGIC ```
# MAGIC
# MAGIC ### Issue: Prophet installation fails
# MAGIC **Solution:** Use cluster with ML Runtime or install via init script
# MAGIC
# MAGIC ### Issue: Power BI connection timeout
# MAGIC **Solution:** Use Import mode instead of DirectQuery, enable incremental refresh
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ POST-DEPLOYMENT VERIFICATION
# MAGIC
# MAGIC ```sql
# MAGIC -- Verify all tables exist
# MAGIC SHOW TABLES IN proyecto1.default LIKE '%bronze%';
# MAGIC SHOW TABLES IN proyecto1.default LIKE '%silver%';
# MAGIC SHOW TABLES IN proyecto1.default LIKE '%ml_%';
# MAGIC SHOW TABLES IN proyecto1.default LIKE '%gold%';
# MAGIC
# MAGIC -- Verify data quality
# MAGIC SELECT 
# MAGIC   'Bronze Records' as layer,
# MAGIC   COUNT(*) as records
# MAGIC FROM proyecto1.default.bronze_modelo_bigdata
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Silver Records', COUNT(*)
# MAGIC FROM proyecto1.default.silver_inventario_recalculado
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ML Forecasts', COUNT(*)
# MAGIC FROM proyecto1.default.ml_demand_forecasts
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Gold Dashboard', COUNT(*)
# MAGIC FROM proyecto1.default.gold_inventory_forecasting_dashboard;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC - Bronze Records: 24,382
# MAGIC - Silver Records: 17,654 (active sites only)
# MAGIC - ML Forecasts: 17,654
# MAGIC - Gold Dashboard: 17,654
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 YOU'RE READY TO DEPLOY!
# MAGIC
# MAGIC Execute the steps above in sequence, validate each layer, and your predictive supply chain system will be operational.

# COMMAND ----------

