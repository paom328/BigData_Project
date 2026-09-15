# Databricks notebook source
# DBTITLE 1,Databricks Workflow Job Configuration
# MAGIC %md
# MAGIC # Databricks Workflow Job Configuration
# MAGIC ## Paper Roll Logistics Daily Pipeline
# MAGIC
# MAGIC **Job Name:** `Paper_Roll_Logistics_Daily_Pipeline`  
# MAGIC **Schedule:** Daily at 05:00 AM UTC  
# MAGIC **Execution:** Sequential task chain with dependency management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Job Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────────┐
# MAGIC │  [TASK 1] Bronze Ingestion                                      │
# MAGIC │  Load Excel files from UC Volume → Bronze Delta Tables         │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC                              ↓
# MAGIC ┌───────────────────────────────────────────────────────────┐
# MAGIC │  [TASK 2] Silver Transformation                                 │
# MAGIC │  Multi-source JOIN + Business Logic + Risk Classification     │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC                              ↓
# MAGIC ┌───────────────────────────────────────────────────────────┐
# MAGIC │  [TASK 3] ML Forecasting                                        │
# MAGIC │  Prophet time-series + Safety Stock + Reorder Points          │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC                              ↓
# MAGIC ┌───────────────────────────────────────────────────────────┐
# MAGIC │  [TASK 4] Gold Aggregation                                      │
# MAGIC │  Executive KPIs + Power BI Optimized Schema                   │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Complete YAML Job Specification
# MAGIC %md
# MAGIC ## Complete YAML Job Specification
# MAGIC
# MAGIC ```yaml
# MAGIC name: Paper_Roll_Logistics_Daily_Pipeline
# MAGIC
# MAGIC tags:
# MAGIC   project: bigdata_logistics
# MAGIC   domain: supply_chain
# MAGIC   environment: production
# MAGIC
# MAGIC schedule:
# MAGIC   quartz_cron_expression: "0 0 5 * * ?"
# MAGIC   timezone_id: "UTC"
# MAGIC   pause_status: "UNPAUSED"
# MAGIC
# MAGIC max_concurrent_runs: 1
# MAGIC
# MAGIC email_notifications:
# MAGIC   on_success:
# MAGIC     - andreys.mendoza7152@unaula.edu.co
# MAGIC   on_failure:
# MAGIC     - andreys.mendoza7152@unaula.edu.co
# MAGIC
# MAGIC job_clusters:
# MAGIC   - job_cluster_key: "logistics_cluster"
# MAGIC     new_cluster:
# MAGIC       spark_version: "14.3.x-scala2.12"
# MAGIC       node_type_id: "Standard_DS3_v2"
# MAGIC       num_workers: 2
# MAGIC       autoscale:
# MAGIC         min_workers: 2
# MAGIC         max_workers: 8
# MAGIC       spark_conf:
# MAGIC         "spark.databricks.delta.optimizeWrite.enabled": "true"
# MAGIC         "spark.databricks.delta.autoCompact.enabled": "true"
# MAGIC       custom_tags:
# MAGIC         project: "bigdata_logistics"
# MAGIC         cost_center: "supply_chain"
# MAGIC
# MAGIC tasks:
# MAGIC   - task_key: "bronze_ingestion"
# MAGIC     job_cluster_key: "logistics_cluster"
# MAGIC     notebook_task:
# MAGIC       notebook_path: "/Users/andreys.mendoza7152@unaula.edu.co/BigData_Project/01_bronze_ingestion"
# MAGIC       base_parameters: {}
# MAGIC     timeout_seconds: 3600
# MAGIC     max_retries: 2
# MAGIC     min_retry_interval_millis: 60000
# MAGIC
# MAGIC   - task_key: "silver_transformation"
# MAGIC     depends_on:
# MAGIC       - task_key: "bronze_ingestion"
# MAGIC     job_cluster_key: "logistics_cluster"
# MAGIC     notebook_task:
# MAGIC       notebook_path: "/Users/andreys.mendoza7152@unaula.edu.co/BigData_Project/02_Silver_Transformation_Production"
# MAGIC       base_parameters: {}
# MAGIC     timeout_seconds: 3600
# MAGIC     max_retries: 2
# MAGIC     min_retry_interval_millis: 60000
# MAGIC
# MAGIC   - task_key: "ml_forecasting"
# MAGIC     depends_on:
# MAGIC       - task_key: "silver_transformation"
# MAGIC     job_cluster_key: "logistics_cluster"
# MAGIC     notebook_task:
# MAGIC       notebook_path: "/Users/andreys.mendoza7152@unaula.edu.co/BigData_Project/03_ml_forecasting"
# MAGIC       base_parameters: {}
# MAGIC     libraries:
# MAGIC       - pypi:
# MAGIC           package: "prophet"
# MAGIC       - pypi:
# MAGIC           package: "mlflow"
# MAGIC     timeout_seconds: 7200
# MAGIC     max_retries: 1
# MAGIC     min_retry_interval_millis: 120000
# MAGIC
# MAGIC   - task_key: "gold_aggregation"
# MAGIC     depends_on:
# MAGIC       - task_key: "ml_forecasting"
# MAGIC     job_cluster_key: "logistics_cluster"
# MAGIC     notebook_task:
# MAGIC       notebook_path: "/Users/andreys.mendoza7152@unaula.edu.co/BigData_Project/04_gold_aggregation"
# MAGIC       base_parameters: {}
# MAGIC     timeout_seconds: 1800
# MAGIC     max_retries: 2
# MAGIC     min_retry_interval_millis: 60000
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Job Deployment Instructions
# MAGIC %md
# MAGIC ## Job Deployment Instructions
# MAGIC
# MAGIC ### Option 1: Deploy via Databricks CLI
# MAGIC
# MAGIC ```bash
# MAGIC # Save the YAML above to a file
# MAGIC cat > paper_roll_logistics_job.yaml << 'EOF'
# MAGIC [paste YAML content here]
# MAGIC EOF
# MAGIC
# MAGIC # Deploy the job
# MAGIC databricks jobs create --json-file paper_roll_logistics_job.yaml
# MAGIC
# MAGIC # Or update existing job
# MAGIC databricks jobs reset --job-id <JOB_ID> --json-file paper_roll_logistics_job.yaml
# MAGIC ```
# MAGIC
# MAGIC ### Option 2: Deploy via Databricks UI
# MAGIC
# MAGIC 1. Navigate to **Workflows** in Databricks workspace
# MAGIC 2. Click **Create Job**
# MAGIC 3. Configure each task manually:
# MAGIC    * **Task 1:** Bronze Ingestion
# MAGIC    * **Task 2:** Silver Transformation (depends on Task 1)
# MAGIC    * **Task 3:** ML Forecasting (depends on Task 2)
# MAGIC    * **Task 4:** Gold Aggregation (depends on Task 3)
# MAGIC 4. Set schedule: Daily at 05:00 AM UTC
# MAGIC 5. Configure email notifications
# MAGIC 6. Save and enable the job
# MAGIC
# MAGIC ### Option 3: Deploy via Python SDK
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC import yaml
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Load YAML config
# MAGIC with open('paper_roll_logistics_job.yaml', 'r') as f:
# MAGIC     job_config = yaml.safe_load(f)
# MAGIC
# MAGIC # Create job
# MAGIC job = w.jobs.create(**job_config)
# MAGIC print(f"Job created with ID: {job.job_id}")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Job Monitoring & Alerts
# MAGIC %md
# MAGIC ## Job Monitoring & Alerts
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC **1. Execution Duration:**
# MAGIC * Expected total runtime: 30-45 minutes
# MAGIC * Bronze: 5-10 minutes
# MAGIC * Silver: 10-15 minutes
# MAGIC * ML Forecasting: 10-20 minutes
# MAGIC * Gold: 5-10 minutes
# MAGIC
# MAGIC **2. Data Quality Metrics:**
# MAGIC * Record count consistency (should match master data: ~24K records)
# MAGIC * Imputation rate (baseline stock assignments)
# MAGIC * Stock update success rate
# MAGIC * Coverage calculation completeness
# MAGIC
# MAGIC **3. Failure Scenarios:**
# MAGIC * 🚨 **Critical:** Bronze ingestion fails (source files missing)
# MAGIC * 🚨 **Critical:** Silver transformation fails (schema mismatch)
# MAGIC * ⚠️ **Warning:** ML forecasting timeout (large dataset)
# MAGIC * ⚠️ **Warning:** Gold aggregation slow (index optimization needed)
# MAGIC
# MAGIC ### Alert Channels
# MAGIC
# MAGIC **Email Notifications:**
# MAGIC * Success: Daily summary report
# MAGIC * Failure: Immediate alert with error details
# MAGIC
# MAGIC **Slack Integration (Optional):**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC def send_slack_alert(status, message):
# MAGIC     webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# MAGIC     payload = {
# MAGIC         "text": f"Pipeline {status}: {message}"
# MAGIC     }
# MAGIC     requests.post(webhook_url, json=payload)
# MAGIC ```
# MAGIC
# MAGIC **PagerDuty Integration (Optional):**
# MAGIC * Critical failures trigger on-call engineer page
# MAGIC * Escalation after 2 consecutive failures

# COMMAND ----------

# DBTITLE 1,Job Permissions & Access Control
# MAGIC %md
# MAGIC ## Job Permissions & Access Control
# MAGIC
# MAGIC ### Required Permissions
# MAGIC
# MAGIC **Job Owner:**
# MAGIC * `CAN MANAGE` permission on the job
# MAGIC * Full control over scheduling, configuration, and runs
# MAGIC
# MAGIC **Data Engineers:**
# MAGIC * `CAN MANAGE RUN` - Can trigger manual runs
# MAGIC * `CAN VIEW` - Can view job configuration and run history
# MAGIC
# MAGIC **Analysts / Stakeholders:**
# MAGIC * `CAN VIEW` - Read-only access to job runs and logs
# MAGIC
# MAGIC ### Unity Catalog Permissions
# MAGIC
# MAGIC **Service Principal / Job Identity:**
# MAGIC ```sql
# MAGIC -- Grant catalog access
# MAGIC GRANT USE CATALOG ON CATALOG catalog_logistica TO `job_principal`;
# MAGIC
# MAGIC -- Grant schema access
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_logistica.db_rollos TO `job_principal`;
# MAGIC
# MAGIC -- Grant table create/modify permissions
# MAGIC GRANT CREATE TABLE ON SCHEMA catalog_logistica.db_rollos TO `job_principal`;
# MAGIC GRANT MODIFY ON SCHEMA catalog_logistica.db_rollos TO `job_principal`;
# MAGIC
# MAGIC -- Grant volume read/write permissions
# MAGIC GRANT READ FILES, WRITE FILES ON VOLUME catalog_logistica.db_rollos.landing TO `job_principal`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Troubleshooting Guide
# MAGIC %md
# MAGIC ## Troubleshooting Guide
# MAGIC
# MAGIC ### Common Issues
# MAGIC
# MAGIC **Issue 1: Bronze Ingestion Fails - "File Not Found"**
# MAGIC * **Cause:** Excel files missing from landing volume
# MAGIC * **Solution:** Verify files exist at `/Volumes/catalog_logistica/db_rollos/landing/`
# MAGIC * **Prevention:** Implement file upload validation step
# MAGIC
# MAGIC **Issue 2: Silver Transformation - "Column Not Found"**
# MAGIC * **Cause:** Source Excel schema changed
# MAGIC * **Solution:** Update column mapping in Silver transformation notebook
# MAGIC * **Prevention:** Add schema validation in Bronze layer
# MAGIC
# MAGIC **Issue 3: ML Forecasting Timeout**
# MAGIC * **Cause:** Dataset too large or insufficient cluster resources
# MAGIC * **Solution:** Increase cluster size or timeout threshold
# MAGIC * **Prevention:** Partition forecasting by DEPARTAMENTO
# MAGIC
# MAGIC **Issue 4: Cluster Startup Failure**
# MAGIC * **Cause:** Cloud provider capacity issues
# MAGIC * **Solution:** Configure job cluster with instance pools
# MAGIC * **Prevention:** Use multi-zone cluster deployment
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC **1. Enable Auto Optimize:**
# MAGIC ```python
# MAGIC spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# MAGIC spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
# MAGIC ```
# MAGIC
# MAGIC **2. Z-Order Clustering:**
# MAGIC ```sql
# MAGIC OPTIMIZE catalog_logistica.db_rollos.silver_inventario_integrado
# MAGIC ZORDER BY (ESTADO_INVENTARIO, DEPARTAMENTO, T);
# MAGIC ```
# MAGIC
# MAGIC **3. Caching Strategy:**
# MAGIC ```python
# MAGIC df_master.cache()
# MAGIC df_stock.cache()
# MAGIC df_oc.cache()
# MAGIC ```

# COMMAND ----------

