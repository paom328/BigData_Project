# Databricks notebook source
# DBTITLE 1,Big Data Inventory & Predictive Logistics Optimization
# MAGIC %md
# MAGIC # Big Data Inventory & Predictive Logistics Optimization
# MAGIC ## Paper Rolls Distribution Network - Colombia
# MAGIC
# MAGIC ### 📊 Project Overview
# MAGIC
# MAGIC **Business Domain:** Supply Chain Logistics & Inventory Management  
# MAGIC **Industry:** Paper Roll Distribution & Print Services Operations  
# MAGIC **Geographic Coverage:** 24,382 Service Points (PUS) across Colombia  
# MAGIC **Data Volume:** Multi-source integration (Master Data + Live Inventory + Requirements Log)
# MAGIC
# MAGIC **Project Lead:** andreys.mendoza7152@unaula.edu.co  
# MAGIC **Workspace:** dbc-43f7a08b-5de1.cloud.databricks.com  
# MAGIC **Last Updated:** September 14, 2026

# COMMAND ----------

# DBTITLE 1,Medallion Architecture
# MAGIC %md
# MAGIC ## 🏗️ Medallion Lakehouse Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  LANDING → BRONZE → SILVER → ML FORECASTING → GOLD → Power BI    │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **LANDING:** `/Volumes/catalog_logistica/db_rollos/landing/`  
# MAGIC **BRONZE:** `catalog_logistica.db_rollos.bronze_*`  
# MAGIC **SILVER:** `catalog_logistica.db_rollos.silver_inventario_integrado`  
# MAGIC **GOLD:** `catalog_logistica.db_rollos.gold_plan_abastecimiento`

# COMMAND ----------

# DBTITLE 1,Core Business Formulas
# MAGIC %md
# MAGIC ## 📐 Core Business Formulas
# MAGIC
# MAGIC ### 1. Roll Balance (Inventory Position)
# MAGIC ```python
# MAGIC SALDO_ROLLOS = ROLLOS_ENTREGADOS - ROLLOS_CONSUMIDOS
# MAGIC ```
# MAGIC
# MAGIC ### 2. Coverage Days (Days of Supply)
# MAGIC ```python
# MAGIC SALDO_DIAS = (SALDO_ROLLOS * 30) / PROM_TRANSACCIONES
# MAGIC ```
# MAGIC **Where:**
# MAGIC * `SALDO_ROLLOS`: Current inventory balance (physical rolls)
# MAGIC * `PROM_TRANSACCIONES`: Average monthly transaction volume
# MAGIC * **Result:** Days until stockout at current consumption rate
# MAGIC
# MAGIC ### 3. Coverage Risk Brackets (Column T)
# MAGIC
# MAGIC | Bracket | Days Range | Risk Level |
# MAGIC |---------|------------|------------|
# MAGIC | DESABASTECIDO | ≤ 0 | 🔴 Critical |
# MAGIC | 1-5 D | 1-5 | 🔴 Urgent |
# MAGIC | 6-10 D | 6-10 | 🟡 Warning |
# MAGIC | 10-15 D | 11-15 | 🟡 Monitor |
# MAGIC | 15-20 D | 16-20 | 🟢 Adequate |
# MAGIC | 21-25 D | 21-25 | 🟢 Good |
# MAGIC | 26-30 D | 26-30 | 🟢 Optimal |
# MAGIC | 31-35 D | 31-35 | 🟢 Healthy |
# MAGIC | 36-45 D | 36-45 | 🔵 Surplus |
# MAGIC | 45-60 D | 46-60 | 🔵 Overstocked |
# MAGIC | 60-90 D | 61-90 | 🔵 Excess |
# MAGIC | 90 D+ | > 90 | 🔵 Strategic Reserve |
# MAGIC
# MAGIC ### 4. Inventory Status Classification
# MAGIC ```python
# MAGIC if SALDO_ROLLOS <= 0 OR site_has_open_purchase_order:
# MAGIC     ESTADO_INVENTARIO = "DESABASTECIDO"
# MAGIC elif SALDO_ROLLOS < 30:
# MAGIC     ESTADO_INVENTARIO = "CRÍTICO"
# MAGIC else:
# MAGIC     ESTADO_INVENTARIO = "ABASTECIDO"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Data Integration Logic
# MAGIC %md
# MAGIC ## 🔄 Data Integration Logic
# MAGIC
# MAGIC ### Multi-Source JOIN Strategy
# MAGIC **Primary Key Mapping:**
# MAGIC * `Modelo_bigdata.cncodpus` (Master) ←→ `Stock_Wompi.CB` (Live Inventory)
# MAGIC * `Modelo_bigdata.cncodpus` (Master) ←→ `OC_AGOSTO.CB` (Requirements)
# MAGIC
# MAGIC **Join Type:** `LEFT JOIN` (preserve all master sites)
# MAGIC
# MAGIC ### Stock Recalculation Rules
# MAGIC
# MAGIC **1. Live Inventory Update:**
# MAGIC * IF `Stock_Wompi.Cantidad` exists for site → Override `ROLLOS_ENTREGADOS` with live count
# MAGIC * ELSE → Retain original `ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA`
# MAGIC
# MAGIC **2. Baseline Imputation (Anti-False-Positive Logic):**
# MAGIC ```python
# MAGIC if (ROLLOS_ENTREGADOS IS NULL OR ROLLOS_ENTREGADOS == 0) AND 
# MAGIC    (site NOT IN OC_AGOSTO purchase orders):
# MAGIC     ROLLOS_ENTREGADOS = 24  # Baseline imputation
# MAGIC     imputed_flag = True
# MAGIC ```
# MAGIC **Rationale:** Sites without active requirements and missing stock data receive baseline inventory to prevent false stockout alerts.

# COMMAND ----------

# DBTITLE 1,Automation & Orchestration
# MAGIC %md
# MAGIC ## ⚙️ Automation & Orchestration
# MAGIC
# MAGIC ### Databricks Workflow Job
# MAGIC **Job Name:** `Paper_Roll_Logistics_Daily_Pipeline`  
# MAGIC **Schedule:** Daily at **05:00 AM UTC** (CRON: `0 5 * * *`)  
# MAGIC **Cluster Strategy:** Shared job cluster (auto-scaling 2-8 workers)
# MAGIC
# MAGIC #### Task Execution Graph
# MAGIC ```
# MAGIC [1] Bronze Ingestion
# MAGIC      ↓
# MAGIC [2] Silver Transformation & Business Logic
# MAGIC      ↓
# MAGIC [3] ML Forecasting & Predictions
# MAGIC      ↓
# MAGIC [4] Gold Aggregation & KPI Generation
# MAGIC      ↓
# MAGIC [5] Data Quality Validation
# MAGIC      ↓
# MAGIC [6] Email Notification
# MAGIC ```
# MAGIC
# MAGIC **Notification Channels:**
# MAGIC * ✅ Success: Logistics team email digest
# MAGIC * ❌ Failure: Page on-call engineer + Slack alert

# COMMAND ----------

# DBTITLE 1,Power BI Dashboard Architecture
# MAGIC %md
# MAGIC ## 📊 Power BI Dashboard Architecture
# MAGIC
# MAGIC ### Executive Command Center (3-Page Design)
# MAGIC
# MAGIC #### **Page 1: Logistics Control Tower**
# MAGIC * **KPI Cards (Top Bar):**
# MAGIC   * Total Service Points (24,382 PUS)
# MAGIC   * Network Health Index %
# MAGIC   * Critical Stock Locations (< 30 rolls)
# MAGIC   * Confirmed Stockouts (cross-referenced with OC AGOSTO)
# MAGIC   * Total Logistics Budget Executed
# MAGIC
# MAGIC * **Geographic Heatmap:** Interactive map of Colombia (DEPARTAMENTO / MUNICIPIO)
# MAGIC   * 🔴 Red: Desabastecido
# MAGIC   * 🟡 Yellow: Crítico (< 30 rolls)
# MAGIC   * 🟢 Green: Abastecido
# MAGIC
# MAGIC * **Donut Chart:** Distribution of ESTADO_INVENTARIO
# MAGIC * **Waterfall Chart:** Freight cost impact across operational hubs
# MAGIC
# MAGIC #### **Page 2: Inventory Coverage Analytics**
# MAGIC * **Stacked Bar Chart:** PUS count by coverage bracket (Column T)
# MAGIC * **Scatter Plot:** Stock vs Transaction Demand (identifies high-risk sites)
# MAGIC * **Predictive Consumption Timeline:** 30-day forecast vs Safety Stock
# MAGIC
# MAGIC #### **Page 3: Operational Dispatch Queue**
# MAGIC * **Action Matrix:** Dispatch priority table ordered by urgency
# MAGIC * **Interactive Slicers:** Filter by DEPARTAMENTO, SEDE ROLLOS, ESTADO_INVENTARIO
# MAGIC * **Conditional Formatting:** Color-coded row highlights
# MAGIC
# MAGIC ### Power BI Connection Parameters
# MAGIC ```
# MAGIC Connection Type: DirectQuery
# MAGIC Server: dbc-43f7a08b-5de1.cloud.databricks.com
# MAGIC HTTP Path: /sql/1.0/warehouses/<warehouse_id>
# MAGIC Catalog: catalog_logistica
# MAGIC Schema: db_rollos
# MAGIC Table: gold_plan_abastecimiento
# MAGIC Refresh: Real-time (on-demand)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Deployment Guide
# MAGIC %md
# MAGIC ## 🚀 Deployment Guide
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC **1. Unity Catalog Setup:**
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_logistica;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_logistica.db_rollos;
# MAGIC CREATE VOLUME IF NOT EXISTS catalog_logistica.db_rollos.landing;
# MAGIC ```
# MAGIC
# MAGIC **2. Permissions:**
# MAGIC * `USE CATALOG` on `catalog_logistica`
# MAGIC * `CREATE TABLE` on `catalog_logistica.db_rollos`
# MAGIC * `WRITE FILES` on volume `landing`
# MAGIC
# MAGIC **3. Cluster Configuration:**
# MAGIC * Runtime: DBR 14.3 LTS ML
# MAGIC * Node Type: Standard_DS3_v2 (or equivalent)
# MAGIC * Auto-scaling: 2-8 workers
# MAGIC * Libraries: `prophet`, `mlflow`, `databricks-sdk`
# MAGIC
# MAGIC ### Installation Steps
# MAGIC
# MAGIC **Step 1:** Upload raw files to landing volume  
# MAGIC **Step 2:** Execute Bronze ingestion notebook  
# MAGIC **Step 3:** Run Silver transformation with business logic  
# MAGIC **Step 4:** Deploy ML forecasting models  
# MAGIC **Step 5:** Generate Gold reporting tables  
# MAGIC **Step 6:** Configure Databricks Workflow Job  
# MAGIC **Step 7:** Connect Power BI using DirectQuery
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📁 Project Structure
# MAGIC
# MAGIC * `00_PROJECT_README` - This documentation
# MAGIC * `02_Silver_Transformation_Production` - Complete PySpark ETL with business rules
# MAGIC * `Workflow_Job_Configuration` - YAML job specification
# MAGIC * `PowerBI_DAX_Measures` - Power BI measures & calculations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Version:** 1.0.0 | September 2026

# COMMAND ----------

