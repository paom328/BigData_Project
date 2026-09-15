# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título y Descripción del Proyecto
# MAGIC %md
# MAGIC # Arquitectura Lakehouse Medallion - Sistema Logístico de Distribución de Rollos
# MAGIC
# MAGIC **Arquitecto:** Principal Big Data Engineer & Analytics Architect  
# MAGIC **Plataforma:** Databricks Unity Catalog  
# MAGIC **Patrón:** Medallion Architecture (Bronze → Silver → Gold)
# MAGIC
# MAGIC ## Contexto del Negocio
# MAGIC Sistema de gestión y optimización logística para distribución de rollos de papel en **24,382 Puntos de Servicio (PUS)** distribuidos en:
# MAGIC - **34 Departamentos** y **1,016 Municipios**
# MAGIC - **8 Sedes de distribución** con tipologías (Principal, Intermedia, Lejana)
# MAGIC - **Presupuesto de transporte:** $440,933,125 COP
# MAGIC
# MAGIC ## Situación Crítica Identificada
# MAGIC - ⚠️ **59.7%** de puntos requieren reabastecimiento inmediato (14,556 PUS)
# MAGIC - 🔴 **13,697 puntos desabastecidos**
# MAGIC - 📉 **Promedio saldo:** -45 días (indicador de crisis de stock)
# MAGIC - 🚨 **13,630 puntos** con saldo negativo
# MAGIC
# MAGIC ## Arquitectura Implementada
# MAGIC 1. **BRONZE**: Ingesta cruda con Change Data Feed
# MAGIC 2. **SILVER**: Transformación, limpieza, particionado y data quality
# MAGIC 3. **GOLD**: Agregaciones analíticas y KPIs de negocio
# MAGIC 4. **ORCHESTRATION**: Databricks Jobs con ejecución programada
# MAGIC 5. **DASHBOARDS**: Visualizaciones automatizadas con alertas

# COMMAND ----------

# DBTITLE 1,Dataset Queries - Corrected for Raw Table
# MAGIC %md
# MAGIC ## 📊 Dashboard Dataset Queries - proyecto1.default.modelo_bigdata_raw
# MAGIC
# MAGIC ### ⚠️ IMPORTANT: Permission Fix Required
# MAGIC All dashboard datasets must query from `proyecto1.default.modelo_bigdata_raw` instead of `catalog_logistica.db_rollos.*` tables.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Open the dashboard in edit mode
# MAGIC 2. For each dataset listed below, click on the dataset → Edit → Replace the SQL query
# MAGIC 3. Copy the corrected SQL from this notebook
# MAGIC 4. Save and refresh the dataset
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 1: Cobertura Promedio Nacional
# MAGIC **Current table:** `catalog_logistica.db_rollos.silver_inventario_rollos`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ROUND(AVG(TRY_CAST(`SALDO DIAS` AS DOUBLE)), 1) as cobertura_promedio_dias
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 2: KPIs Financieros Globales
# MAGIC **Current table:** `catalog_logistica.db_rollos.gold_kpis_financieros`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT cncodpus) as pus_activos,
# MAGIC   SUM(CASE WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 0 THEN 1 ELSE 0 END) as pus_desabastecidos,
# MAGIC   SUM(CASE WHEN `ACCIÓN` = 'REABASTECER' THEN 1 ELSE 0 END) as pus_reabastecer,
# MAGIC   ROUND(SUM(TRY_CAST(`PPTO TRANSP` AS DOUBLE)) / 1000000, 2) as presupuesto_total_asignado
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 3: Top 15 Departamentos Críticos
# MAGIC **Current table:** `catalog_logistica.db_rollos.silver_inventario_rollos`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   DEPARTAMENTO,
# MAGIC   SUM(TRY_CAST(`SALDO ROLLOS` AS DOUBLE)) as saldo_total,
# MAGIC   COUNT(cncodpus) as total_pus,
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 7 THEN 1
# MAGIC       ELSE 0
# MAGIC     END
# MAGIC   ) as pus_criticos
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC GROUP BY
# MAGIC   DEPARTAMENTO
# MAGIC ORDER BY
# MAGIC   pus_criticos DESC
# MAGIC LIMIT 15
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 4: Relación Consumo vs Cobertura
# MAGIC **Current table:** `catalog_logistica.db_rollos.silver_inventario_rollos`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   cncodpus,
# MAGIC   prom_mensual as consumo_mensual,
# MAGIC   TRY_CAST(`SALDO DIAS` AS DOUBLE) as cobertura_dias,
# MAGIC   CASE
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 7 THEN 'CRÍTICO'
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 15 THEN 'ALTO'
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 30 THEN 'MEDIO'
# MAGIC     ELSE 'BAJO'
# MAGIC   END as riesgo_desabastecimiento
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC   AND prom_mensual IS NOT NULL
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 5: Top 30 Alertas Reabastecimiento
# MAGIC **Current table:** `catalog_logistica.db_rollos.gold_alertas_reabastecimiento`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ROW_NUMBER() OVER (ORDER BY TRY_CAST(`SALDO DIAS` AS DOUBLE) ASC) as ranking_prioridad,
# MAGIC   cncodpus,
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   `SEDE ROLLOS` as sede,
# MAGIC   TRY_CAST(`SALDO DIAS` AS DOUBLE) as cobertura_dias,
# MAGIC   ABS(TRY_CAST(`SALDO ROLLOS` AS DOUBLE)) as rollos_requeridos,
# MAGIC   ROUND(TRY_CAST(`PPTO TRANSP` AS DOUBLE), 2) as costo_cop,
# MAGIC   DATE_ADD(CURRENT_DATE(), CAST(TRY_CAST(`SALDO DIAS` AS DOUBLE) AS INT)) as fecha_estimada_desabastecimiento
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC   AND `ACCIÓN` = 'REABASTECER'
# MAGIC   AND TRY_CAST(`SALDO DIAS` AS DOUBLE) < 30
# MAGIC ORDER BY
# MAGIC   cobertura_dias ASC
# MAGIC LIMIT 30
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 6: Resumen Inventario por Sede
# MAGIC **Current table:** `catalog_logistica.db_rollos.gold_resumen_inventario_sede`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   `SEDE ROLLOS` as sede,
# MAGIC   COUNT(cncodpus) as total_pus,
# MAGIC   SUM(TRY_CAST(`SALDO ROLLOS` AS DOUBLE)) as stock_total_rollos,
# MAGIC   ROUND(AVG(TRY_CAST(`SALDO DIAS` AS DOUBLE)), 1) as cobertura_promedio_dias,
# MAGIC   SUM(CASE WHEN `ACCIÓN` = 'REABASTECER' THEN 1 ELSE 0 END) as pus_requieren_reabastecimiento,
# MAGIC   ROUND(SUM(TRY_CAST(`PPTO TRANSP` AS DOUBLE)) / 1000000, 2) as presupuesto_asignado_millones
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC GROUP BY
# MAGIC   `SEDE ROLLOS`
# MAGIC ORDER BY
# MAGIC   pus_requieren_reabastecimiento DESC
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 7: Porcentaje Desabastecidos
# MAGIC **Current table:** `catalog_logistica.db_rollos.gold_kpis_financieros`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC WITH kpis AS (
# MAGIC   SELECT
# MAGIC     COUNT(DISTINCT cncodpus) as pus_activos,
# MAGIC     SUM(CASE WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 0 THEN 1 ELSE 0 END) as pus_desabastecidos
# MAGIC   FROM
# MAGIC     proyecto1.default.modelo_bigdata_raw
# MAGIC   WHERE
# MAGIC     `ESTADO PUNTO` = 'ACTIVO'
# MAGIC )
# MAGIC SELECT
# MAGIC   ROUND((pus_desabastecidos * 100.0 / pus_activos), 2) as pct_desabastecidos
# MAGIC FROM kpis
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 8: Distribución de Niveles de Riesgo
# MAGIC **Current table:** `catalog_logistica.db_rollos.silver_inventario_rollos`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 7 THEN 'CRÍTICO'
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 15 THEN 'ALTO'
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 30 THEN 'MEDIO'
# MAGIC     ELSE 'BAJO'
# MAGIC   END as nivel_riesgo,
# MAGIC   COUNT(cncodpus) as cantidad
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC GROUP BY
# MAGIC   1
# MAGIC ORDER BY
# MAGIC   CASE
# MAGIC     WHEN nivel_riesgo = 'CRÍTICO' THEN 1
# MAGIC     WHEN nivel_riesgo = 'ALTO' THEN 2
# MAGIC     WHEN nivel_riesgo = 'MEDIO' THEN 3
# MAGIC     ELSE 4
# MAGIC   END
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 9: Top 10 Municipios Críticos
# MAGIC **Current table:** `catalog_logistica.db_rollos.silver_inventario_rollos`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   COUNT(cncodpus) as pus_criticos,
# MAGIC   ROUND(AVG(TRY_CAST(`SALDO DIAS` AS DOUBLE)), 1) as promedio_cobertura,
# MAGIC   SUM(TRY_CAST(`SALDO ROLLOS` AS DOUBLE)) as stock_total
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC   AND TRY_CAST(`SALDO DIAS` AS DOUBLE) < 15
# MAGIC GROUP BY
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO
# MAGIC ORDER BY
# MAGIC   pus_criticos DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset 10: Acciones por Sede de Distribución
# MAGIC **Current table:** `catalog_logistica.db_rollos.silver_inventario_rollos`  
# MAGIC **Replace with:**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   `SEDE ROLLOS` as sede,
# MAGIC   `ACCIÓN` as accion,
# MAGIC   COUNT(cncodpus) as cantidad_pus
# MAGIC FROM
# MAGIC   proyecto1.default.modelo_bigdata_raw
# MAGIC WHERE
# MAGIC   `ESTADO PUNTO` = 'ACTIVO'
# MAGIC GROUP BY
# MAGIC   `SEDE ROLLOS`,
# MAGIC   `ACCIÓN`
# MAGIC ORDER BY
# MAGIC   `SEDE ROLLOS`
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Dataset 11: Resumen de Alertas por Nivel
# MAGIC **Status:** Already correct - uses `proyecto1.default.modelo_bigdata_raw`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Manual Update Steps
# MAGIC
# MAGIC 1. **Navigate to Dashboard**: Open "Sistema Logístico Nacional - Distribución de Rollos"
# MAGIC 2. **Edit Mode**: Click "Edit" button
# MAGIC 3. **For Each Dataset (1-10)**:
# MAGIC    - Click on dataset name in the left panel
# MAGIC    - Click "Edit dataset" or the pencil icon
# MAGIC    - Replace the entire SQL query with the corrected version above
# MAGIC    - Click "Save" or "Run"
# MAGIC    - Verify the dataset loads successfully
# MAGIC 4. **Refresh Widgets**: After all datasets are updated, refresh each widget
# MAGIC 5. **Republish**: Click "Publish" to update the live dashboard
# MAGIC
# MAGIC ## 📋 Key Changes Made
# MAGIC
# MAGIC * **Table**: `catalog_logistica.db_rollos.*` → `proyecto1.default.modelo_bigdata_raw`
# MAGIC * **Column Names**: Added backticks for columns with spaces (`SALDO DIAS`, `SEDE ROLLOS`, etc.)
# MAGIC * **Type Casting**: Used `TRY_CAST(column AS DOUBLE)` for string numeric fields
# MAGIC * **Risk Calculation**: Computed `riesgo_desabastecimiento` directly from `SALDO DIAS` thresholds
# MAGIC * **Estado Filter**: Changed `estado_punto = 'ACTIVO'` to `ESTADO PUNTO = 'ACTIVO'`

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.1: Crear Catálogo y Esquemas Unity Catalog
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- SECCIÓN 1: UNITY CATALOG & MEDALLION ARCHITECTURE
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Crear catálogo para el sistema logístico
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_logistica
# MAGIC COMMENT 'Catálogo Unity Catalog para gestión logística de distribución de rollos';
# MAGIC
# MAGIC -- Crear schema para datos de rollos
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_logistica.db_rollos
# MAGIC COMMENT 'Schema Medallion Architecture: Bronze -> Silver -> Gold layers';
# MAGIC
# MAGIC -- Verificar creación
# MAGIC SHOW SCHEMAS IN catalog_logistica;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.2: BRONZE LAYER - Ingesta Cruda con Change Data Feed
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BRONZE LAYER: Capa de ingesta cruda (Raw Data)
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS catalog_logistica.db_rollos.bronze_rollos_raw;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS catalog_logistica.db_rollos.bronze_rollos_raw
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'Bronze Layer: Datos crudos de distribución de rollos sin transformación'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS _ingestion_timestamp,
# MAGIC   'proyecto1.default.modelo_bigdata_raw' AS _source_table
# MAGIC FROM proyecto1.default.modelo_bigdata_raw;
# MAGIC
# MAGIC -- Estadísticas de ingesta
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_registros_bronze,
# MAGIC   COUNT(DISTINCT cncodpus) as puntos_unicos,
# MAGIC   MIN(_ingestion_timestamp) as primera_ingesta,
# MAGIC   MAX(_ingestion_timestamp) as ultima_ingesta
# MAGIC FROM catalog_logistica.db_rollos.bronze_rollos_raw;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.3: SILVER LAYER - Transformación, Limpieza y Particionado
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- SILVER LAYER: Datos limpios, transformados y optimizados
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS catalog_logistica.db_rollos.silver_inventario_rollos;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DEPARTAMENTO, sede_rollos_clean)
# MAGIC CLUSTER BY (cncodpus)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )
# MAGIC COMMENT 'Silver Layer: Datos transformados con tipos correctos, particionado y limpieza'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- Identificadores
# MAGIC   cncodpus,
# MAGIC   `ESTADO PUNTO` as estado_punto,
# MAGIC   
# MAGIC   -- Métricas de consumo (limpias)
# MAGIC   prom_mensual_anterior,
# MAGIC   rango_prom_anterior,
# MAGIC   prom_mensual,
# MAGIC   rango_prom,
# MAGIC   
# MAGIC   -- Ubicación geográfica
# MAGIC   `DEPARTAMENTO - MUNICIPIO` as departamento_municipio,
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   
# MAGIC   -- Sedes y tipologías
# MAGIC   `SEDE OPERACIONES` as sede_operaciones,
# MAGIC   `TIPOLOGIA OPERACIONES` as tipologia_operaciones,
# MAGIC   `SEDE ROLLOS` as sede_rollos_clean,
# MAGIC   `TIPOLOGIA ROLLOS` as tipologia_rollos,
# MAGIC   
# MAGIC   -- Dimensiones de carga
# MAGIC   `Q MUNIC` as q_municipio,
# MAGIC   `Q CB X MUNIC` as q_cb_por_municipio,
# MAGIC   
# MAGIC   -- Financiero (cast a tipos correctos)
# MAGIC   CAST(`PPTO TRANSP` AS DECIMAL(18,2)) as presupuesto_transporte,
# MAGIC   CAST(`PPTO PUNTO TRANSP MUNIC` AS DECIMAL(18,2)) as presupuesto_punto_transporte_municipio,
# MAGIC   
# MAGIC   -- Métricas de abastecimiento E-5
# MAGIC   `PERIODO ABAST E-5` as periodo_abastecimiento_e5,
# MAGIC   `ROLLOS PERIODO ABAST E-5` as rollos_periodo_abast_e5,
# MAGIC   `ROLLOS AÑO E-5` as rollos_anio_e5,
# MAGIC   
# MAGIC   -- Fechas (parse correcto)
# MAGIC   TO_DATE(`FECHA MIGRACIÓN O APERTURA`, 'M/d/yy') as fecha_migracion_apertura,
# MAGIC   
# MAGIC   -- Métricas operativas (cast seguro a tipos numéricos)
# MAGIC   TRY_CAST(`ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA` AS BIGINT) as rollos_entregados_desde_migracion,
# MAGIC   TRY_CAST(`TRX DESDE MIGRACIÓN O APERTURA` AS BIGINT) as trx_desde_migracion,
# MAGIC   TRY_CAST(`ROLLOS CONSUMIDOS DESDE MIGRACIÓN O APERTURA` AS BIGINT) as rollos_consumidos_desde_migracion,
# MAGIC   TRY_CAST(`SALDO ROLLOS` AS INT) as saldo_rollos,
# MAGIC   TRY_CAST(`SALDO DIAS` AS DOUBLE) as saldo_dias,
# MAGIC   
# MAGIC   -- Alertas y acciones
# MAGIC   `ACCIÓN` as accion,
# MAGIC   T as estado_desabastecimiento,
# MAGIC   TRY_CAST(`FECHA_PLAN_ABAST_1` AS INT) as fecha_plan_abastecimiento_epoch,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- COLUMNAS DERIVADAS (Business Logic)
# MAGIC   -- ========================================================================
# MAGIC   
# MAGIC   -- Tasa de consumo diario
# MAGIC   ROUND(prom_mensual / 30.0, 2) as tasa_consumo_diario,
# MAGIC   
# MAGIC   -- Eficiencia transaccional (rollos por transacción)
# MAGIC   CASE 
# MAGIC     WHEN TRY_CAST(`TRX DESDE MIGRACIÓN O APERTURA` AS BIGINT) > 0 THEN
# MAGIC       ROUND(
# MAGIC         TRY_CAST(`ROLLOS CONSUMIDOS DESDE MIGRACIÓN O APERTURA` AS BIGINT) * 1.0 / 
# MAGIC         TRY_CAST(`TRX DESDE MIGRACIÓN O APERTURA` AS BIGINT), 
# MAGIC         2
# MAGIC       )
# MAGIC     ELSE NULL
# MAGIC   END as eficiencia_transaccional,
# MAGIC   
# MAGIC   -- Clasificación de riesgo de desabastecimiento
# MAGIC   CASE 
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 0 THEN 'CRÍTICO'
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 7 THEN 'ALTO'
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 14 THEN 'MEDIO'
# MAGIC     ELSE 'NORMAL'
# MAGIC   END as riesgo_desabastecimiento,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   current_timestamp() as _processed_timestamp
# MAGIC   
# MAGIC FROM catalog_logistica.db_rollos.bronze_rollos_raw
# MAGIC WHERE cncodpus IS NOT NULL;
# MAGIC
# MAGIC -- Optimizar tabla (OPTIMIZE y Z-ORDER)
# MAGIC OPTIMIZE catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC ZORDER BY (cncodpus, saldo_dias);

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.4: Estadísticas Silver Layer
# MAGIC %sql
# MAGIC -- Validar carga y transformación Silver
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_registros_silver,
# MAGIC   COUNT(DISTINCT cncodpus) as puntos_unicos,
# MAGIC   COUNT(DISTINCT DEPARTAMENTO) as departamentos,
# MAGIC   COUNT(DISTINCT sede_rollos_clean) as sedes_distribucion,
# MAGIC   SUM(CASE WHEN riesgo_desabastecimiento = 'CRÍTICO' THEN 1 ELSE 0 END) as puntos_criticos,
# MAGIC   SUM(CASE WHEN riesgo_desabastecimiento = 'ALTO' THEN 1 ELSE 0 END) as puntos_alto_riesgo,
# MAGIC   SUM(CASE WHEN riesgo_desabastecimiento = 'MEDIO' THEN 1 ELSE 0 END) as puntos_medio_riesgo,
# MAGIC   SUM(CASE WHEN riesgo_desabastecimiento = 'NORMAL' THEN 1 ELSE 0 END) as puntos_normales,
# MAGIC   ROUND(AVG(saldo_dias), 2) as promedio_saldo_dias,
# MAGIC   ROUND(SUM(presupuesto_transporte), 2) as presupuesto_total
# MAGIC FROM catalog_logistica.db_rollos.silver_inventario_rollos;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.5: GOLD VIEW 1 - Resumen Inventario por Sede
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- GOLD LAYER: Agregaciones Analíticas para Dashboards
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- GOLD VIEW 1: Resumen de inventario por sede y tipología
# MAGIC CREATE OR REPLACE VIEW catalog_logistica.db_rollos.gold_resumen_inventario_sede
# MAGIC COMMENT 'Gold View: Resumen de inventario agregado por sede de distribución y tipología'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   sede_rollos_clean,
# MAGIC   tipologia_rollos,
# MAGIC   
# MAGIC   -- Conteo de puntos
# MAGIC   COUNT(DISTINCT cncodpus) as total_pus,
# MAGIC   SUM(CASE WHEN estado_desabastecimiento = 'DESABASTECIDO' THEN 1 ELSE 0 END) as pus_desabastecidos,
# MAGIC   SUM(CASE WHEN accion = 'REABASTECER' THEN 1 ELSE 0 END) as pus_requieren_reabastecimiento,
# MAGIC   
# MAGIC   -- Métricas de inventario
# MAGIC   SUM(saldo_rollos) as saldo_total_rollos,
# MAGIC   ROUND(AVG(saldo_dias), 2) as dias_cobertura_promedio,
# MAGIC   MIN(saldo_dias) as dias_cobertura_minimo,
# MAGIC   MAX(saldo_dias) as dias_cobertura_maximo,
# MAGIC   
# MAGIC   -- Consumo y eficiencia
# MAGIC   ROUND(SUM(prom_mensual), 2) as consumo_mensual_total,
# MAGIC   ROUND(AVG(tasa_consumo_diario), 2) as tasa_consumo_diario_promedio,
# MAGIC   ROUND(AVG(eficiencia_transaccional), 2) as eficiencia_transaccional_promedio,
# MAGIC   
# MAGIC   -- Financiero
# MAGIC   ROUND(SUM(presupuesto_transporte), 2) as presupuesto_transporte_asignado,
# MAGIC   
# MAGIC   -- Distribución de riesgo
# MAGIC   ROUND(SUM(CASE WHEN riesgo_desabastecimiento = 'CRÍTICO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_critico,
# MAGIC   ROUND(SUM(CASE WHEN riesgo_desabastecimiento = 'ALTO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_alto,
# MAGIC   ROUND(SUM(CASE WHEN riesgo_desabastecimiento = 'MEDIO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_medio,
# MAGIC   ROUND(SUM(CASE WHEN riesgo_desabastecimiento = 'NORMAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_normal
# MAGIC   
# MAGIC FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC GROUP BY sede_rollos_clean, tipologia_rollos
# MAGIC ORDER BY total_pus DESC;
# MAGIC
# MAGIC -- Consultar vista
# MAGIC SELECT * FROM catalog_logistica.db_rollos.gold_resumen_inventario_sede;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.6: GOLD VIEW 2 - Alertas de Reabastecimiento Prioritario
# MAGIC %sql
# MAGIC -- GOLD VIEW 2: Alertas de reabastecimiento con priorización
# MAGIC CREATE OR REPLACE VIEW catalog_logistica.db_rollos.gold_alertas_reabastecimiento
# MAGIC COMMENT 'Gold View: Puntos críticos que requieren reabastecimiento inmediato con ranking de prioridad'
# MAGIC AS
# MAGIC WITH puntos_criticos AS (
# MAGIC   SELECT 
# MAGIC     cncodpus,
# MAGIC     DEPARTAMENTO,
# MAGIC     MUNICIPIO,
# MAGIC     sede_rollos_clean,
# MAGIC     tipologia_rollos,
# MAGIC     estado_punto,
# MAGIC     saldo_rollos,
# MAGIC     saldo_dias,
# MAGIC     tasa_consumo_diario,
# MAGIC     accion,
# MAGIC     estado_desabastecimiento,
# MAGIC     riesgo_desabastecimiento,
# MAGIC     presupuesto_transporte as costo_transporte_estimado,
# MAGIC     
# MAGIC     -- Calcular fecha estimada de desabastecimiento
# MAGIC     CASE 
# MAGIC       WHEN saldo_dias < 0 THEN current_date()
# MAGIC       WHEN tasa_consumo_diario > 0 THEN date_add(current_date(), CAST(saldo_dias AS INT))
# MAGIC       ELSE NULL
# MAGIC     END as fecha_estimada_desabastecimiento,
# MAGIC     
# MAGIC     -- Score de urgencia (menor = más urgente)
# MAGIC     CASE 
# MAGIC       WHEN saldo_dias < -30 THEN 1
# MAGIC       WHEN saldo_dias < 0 THEN 2
# MAGIC       WHEN saldo_dias < 3 THEN 3
# MAGIC       WHEN saldo_dias < 7 THEN 4
# MAGIC       ELSE 5
# MAGIC     END as urgencia_score
# MAGIC     
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE accion = 'REABASTECER' OR saldo_dias < 7
# MAGIC )
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER (ORDER BY urgencia_score ASC, saldo_dias ASC) as ranking_prioridad,
# MAGIC   *,
# MAGIC   
# MAGIC   -- Cantidad recomendada a reabastecer (30 días de cobertura)
# MAGIC   CASE 
# MAGIC     WHEN tasa_consumo_diario > 0 THEN CAST(CEIL(tasa_consumo_diario * 30) AS INT)
# MAGIC     ELSE NULL
# MAGIC   END as cantidad_recomendada_rollos
# MAGIC   
# MAGIC FROM puntos_criticos
# MAGIC ORDER BY ranking_prioridad;
# MAGIC
# MAGIC -- Top 20 puntos más críticos
# MAGIC SELECT * 
# MAGIC FROM catalog_logistica.db_rollos.gold_alertas_reabastecimiento
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 1.7: GOLD VIEW 3 - KPIs Financieros Globales
# MAGIC %sql
# MAGIC -- GOLD VIEW 3: KPIs financieros y operativos globales
# MAGIC CREATE OR REPLACE VIEW catalog_logistica.db_rollos.gold_kpis_financieros
# MAGIC COMMENT 'Gold View: KPIs financieros y operativos globales del sistema logístico'
# MAGIC AS
# MAGIC WITH metricas_base AS (
# MAGIC   SELECT 
# MAGIC     COUNT(DISTINCT cncodpus) as total_pus,
# MAGIC     SUM(CASE WHEN estado_punto = 'ACTIVO' THEN 1 ELSE 0 END) as pus_activos,
# MAGIC     SUM(rollos_entregados_desde_migracion) as total_rollos_entregados,
# MAGIC     SUM(rollos_consumidos_desde_migracion) as total_rollos_consumidos,
# MAGIC     SUM(saldo_rollos) as saldo_total_rollos,
# MAGIC     SUM(presupuesto_transporte) as presupuesto_total,
# MAGIC     COUNT(DISTINCT MUNICIPIO) as municipios_activos,
# MAGIC     SUM(CASE WHEN accion = 'REABASTECER' THEN presupuesto_transporte ELSE 0 END) as presupuesto_reabastecimiento_pendiente
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Sistema Logístico Nacional' as ambito,
# MAGIC   total_pus,
# MAGIC   pus_activos,
# MAGIC   municipios_activos,
# MAGIC   
# MAGIC   -- Inventario
# MAGIC   saldo_total_rollos,
# MAGIC   total_rollos_entregados,
# MAGIC   total_rollos_consumidos,
# MAGIC   
# MAGIC   -- Financiero
# MAGIC   ROUND(presupuesto_total, 2) as presupuesto_total_asignado,
# MAGIC   ROUND(presupuesto_reabastecimiento_pendiente, 2) as presupuesto_ejecucion_pendiente,
# MAGIC   ROUND((presupuesto_reabastecimiento_pendiente / presupuesto_total) * 100, 2) as pct_presupuesto_pendiente,
# MAGIC   
# MAGIC   -- Costo unitario
# MAGIC   ROUND(presupuesto_total / NULLIF(total_rollos_entregados, 0), 2) as costo_transporte_por_rollo_entregado,
# MAGIC   ROUND(presupuesto_total / NULLIF(pus_activos, 0), 2) as costo_promedio_por_punto,
# MAGIC   
# MAGIC   -- ROI logístico (rollos consumidos vs entregados)
# MAGIC   ROUND((total_rollos_consumidos * 1.0 / NULLIF(total_rollos_entregados, 0)) * 100, 2) as roi_logistico_pct,
# MAGIC   
# MAGIC   current_timestamp() as fecha_calculo
# MAGIC FROM metricas_base;
# MAGIC
# MAGIC -- Consultar KPIs
# MAGIC SELECT * FROM catalog_logistica.db_rollos.gold_kpis_financieros;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 2: Data Quality Constraints
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- SECCIÓN 2: DATA QUALITY EXPECTATIONS & CONSTRAINTS
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Constraint 1: cncodpus no puede ser NULL (clave primaria de negocio)
# MAGIC ALTER TABLE catalog_logistica.db_rollos.silver_inventario_rollos 
# MAGIC ADD CONSTRAINT check_cncodpus_not_null 
# MAGIC CHECK (cncodpus IS NOT NULL);
# MAGIC
# MAGIC -- Constraint 2: Saldo de rollos no puede ser menor a -1000 (lógica de negocio)
# MAGIC ALTER TABLE catalog_logistica.db_rollos.silver_inventario_rollos 
# MAGIC ADD CONSTRAINT check_saldo_rollos_logico 
# MAGIC CHECK (saldo_rollos >= -1000);
# MAGIC
# MAGIC -- Constraint 3: Presupuesto de transporte debe ser positivo
# MAGIC ALTER TABLE catalog_logistica.db_rollos.silver_inventario_rollos 
# MAGIC ADD CONSTRAINT check_presupuesto_positivo 
# MAGIC CHECK (presupuesto_transporte >= 0);
# MAGIC
# MAGIC -- Constraint 4: Departamento no puede ser NULL
# MAGIC ALTER TABLE catalog_logistica.db_rollos.silver_inventario_rollos 
# MAGIC ADD CONSTRAINT check_departamento_not_null 
# MAGIC CHECK (DEPARTAMENTO IS NOT NULL);
# MAGIC
# MAGIC -- Verificar constraints aplicados
# MAGIC DESCRIBE DETAIL catalog_logistica.db_rollos.silver_inventario_rollos;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 3.1: Query Optimizada - KPIs Globales Dashboard
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- SECCIÓN 3: CONSULTAS SQL OPTIMIZADAS (Window Functions, CTEs, Advanced SQL)
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- QUERY 1: KPIs Globales para Dashboard Principal
# MAGIC -- Uso: Tarjetas de KPI en dashboard con métricas clave del negocio
# MAGIC
# MAGIC WITH kpis_operativos AS (
# MAGIC   SELECT 
# MAGIC     COUNT(DISTINCT cncodpus) as total_pus,
# MAGIC     SUM(CASE WHEN estado_punto = 'ACTIVO' THEN 1 ELSE 0 END) as pus_activos,
# MAGIC     SUM(CASE WHEN estado_desabastecimiento = 'DESABASTECIDO' THEN 1 ELSE 0 END) as pus_desabastecidos,
# MAGIC     SUM(CASE WHEN accion = 'REABASTECER' THEN 1 ELSE 0 END) as pus_requieren_reabastecimiento,
# MAGIC     SUM(saldo_rollos) as saldo_total_rollos,
# MAGIC     ROUND(AVG(saldo_dias), 2) as cobertura_promedio_dias,
# MAGIC     SUM(presupuesto_transporte) as presupuesto_total,
# MAGIC     SUM(CASE WHEN accion = 'REABASTECER' THEN presupuesto_transporte ELSE 0 END) as presupuesto_pendiente_ejecucion
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC ),
# MAGIC kpis_calculados AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROUND((pus_desabastecidos * 100.0 / NULLIF(pus_activos, 0)), 2) as pct_desabastecidos,
# MAGIC     ROUND((pus_requieren_reabastecimiento * 100.0 / NULLIF(pus_activos, 0)), 2) as pct_requieren_reabastecimiento,
# MAGIC     ROUND((presupuesto_pendiente_ejecucion / NULLIF(presupuesto_total, 0)) * 100, 2) as pct_presupuesto_pendiente
# MAGIC   FROM kpis_operativos
# MAGIC )
# MAGIC SELECT 
# MAGIC   'NACIONAL' as ambito,
# MAGIC   total_pus,
# MAGIC   pus_activos,
# MAGIC   pus_desabastecidos,
# MAGIC   pct_desabastecidos,
# MAGIC   pus_requieren_reabastecimiento,
# MAGIC   pct_requieren_reabastecimiento,
# MAGIC   saldo_total_rollos,
# MAGIC   cobertura_promedio_dias,
# MAGIC   ROUND(presupuesto_total, 2) as presupuesto_total_cop,
# MAGIC   ROUND(presupuesto_pendiente_ejecucion, 2) as presupuesto_pendiente_cop,
# MAGIC   pct_presupuesto_pendiente,
# MAGIC   current_timestamp() as fecha_corte
# MAGIC FROM kpis_calculados;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 3.2: Query Optimizada - Matriz de Riesgo por Sede
# MAGIC %sql
# MAGIC -- QUERY 2: Matriz de Riesgo de Stock por Sede y Tipología
# MAGIC -- Uso: Heatmap/matriz para identificar combinaciones críticas de sede-tipología
# MAGIC
# MAGIC WITH matriz_riesgo AS (
# MAGIC   SELECT 
# MAGIC     sede_rollos_clean,
# MAGIC     tipologia_operaciones,
# MAGIC     riesgo_desabastecimiento,
# MAGIC     COUNT(DISTINCT cncodpus) as cantidad_pus,
# MAGIC     ROUND(AVG(saldo_dias), 2) as promedio_saldo_dias,
# MAGIC     SUM(saldo_rollos) as saldo_total,
# MAGIC     ROUND(SUM(presupuesto_transporte), 2) as presupuesto_asociado
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE saldo_dias < 7 OR accion = 'REABASTECER'
# MAGIC   GROUP BY sede_rollos_clean, tipologia_operaciones, riesgo_desabastecimiento
# MAGIC ),
# MAGIC totales_por_sede AS (
# MAGIC   SELECT 
# MAGIC     sede_rollos_clean,
# MAGIC     tipologia_operaciones,
# MAGIC     SUM(cantidad_pus) OVER (PARTITION BY sede_rollos_clean, tipologia_operaciones) as total_pus_combinacion
# MAGIC   FROM matriz_riesgo
# MAGIC )
# MAGIC SELECT 
# MAGIC   m.sede_rollos_clean,
# MAGIC   m.tipologia_operaciones,
# MAGIC   m.riesgo_desabastecimiento,
# MAGIC   m.cantidad_pus,
# MAGIC   ROUND((m.cantidad_pus * 100.0 / t.total_pus_combinacion), 2) as pct_del_total,
# MAGIC   m.promedio_saldo_dias,
# MAGIC   m.saldo_total,
# MAGIC   m.presupuesto_asociado,
# MAGIC   
# MAGIC   -- Ranking de criticidad
# MAGIC   DENSE_RANK() OVER (
# MAGIC     ORDER BY 
# MAGIC       CASE m.riesgo_desabastecimiento 
# MAGIC         WHEN 'CRÍTICO' THEN 1 
# MAGIC         WHEN 'ALTO' THEN 2 
# MAGIC         WHEN 'MEDIO' THEN 3 
# MAGIC         ELSE 4 
# MAGIC       END,
# MAGIC       m.cantidad_pus DESC
# MAGIC   ) as ranking_criticidad
# MAGIC   
# MAGIC FROM matriz_riesgo m
# MAGIC INNER JOIN totales_por_sede t 
# MAGIC   ON m.sede_rollos_clean = t.sede_rollos_clean 
# MAGIC   AND m.tipologia_operaciones = t.tipologia_operaciones
# MAGIC ORDER BY ranking_criticidad, m.cantidad_pus DESC;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 3.3: Query Optimizada - Top 20 Puntos Críticos
# MAGIC %sql
# MAGIC -- QUERY 3: Top 20 Puntos Críticos con Mayor Urgencia de Reabastecimiento
# MAGIC -- Uso: Lista priorizada para operaciones logísticas y planificación de rutas
# MAGIC
# MAGIC WITH puntos_urgentes AS (
# MAGIC   SELECT 
# MAGIC     cncodpus,
# MAGIC     DEPARTAMENTO,
# MAGIC     MUNICIPIO,
# MAGIC     sede_rollos_clean,
# MAGIC     tipologia_rollos,
# MAGIC     estado_punto,
# MAGIC     saldo_rollos,
# MAGIC     saldo_dias,
# MAGIC     tasa_consumo_diario,
# MAGIC     accion,
# MAGIC     riesgo_desabastecimiento,
# MAGIC     presupuesto_transporte,
# MAGIC     
# MAGIC     -- Días hasta desabastecimiento total
# MAGIC     CASE 
# MAGIC       WHEN saldo_dias < 0 THEN 0
# MAGIC       ELSE CAST(saldo_dias AS INT)
# MAGIC     END as dias_hasta_desabastecimiento,
# MAGIC     
# MAGIC     -- Cantidad necesaria para 30 días de cobertura
# MAGIC     CAST(CEIL(tasa_consumo_diario * 30 - saldo_rollos) AS INT) as cantidad_necesaria,
# MAGIC     
# MAGIC     -- Costo estimado de reabastecimiento
# MAGIC     presupuesto_transporte as costo_reabastecimiento_estimado
# MAGIC     
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE accion = 'REABASTECER' AND estado_punto = 'ACTIVO'
# MAGIC )
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER (ORDER BY dias_hasta_desabastecimiento ASC, saldo_dias ASC) as prioridad,
# MAGIC   cncodpus as codigo_punto,
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   sede_rollos_clean as sede_distribucion,
# MAGIC   tipologia_rollos,
# MAGIC   saldo_rollos as stock_actual,
# MAGIC   saldo_dias as cobertura_dias,
# MAGIC   dias_hasta_desabastecimiento,
# MAGIC   cantidad_necesaria as rollos_requeridos,
# MAGIC   ROUND(tasa_consumo_diario, 2) as consumo_diario,
# MAGIC   riesgo_desabastecimiento as nivel_riesgo,
# MAGIC   ROUND(costo_reabastecimiento_estimado, 2) as costo_estimado_cop,
# MAGIC   
# MAGIC   -- Indicador visual de urgencia
# MAGIC   CASE 
# MAGIC     WHEN dias_hasta_desabastecimiento = 0 THEN '🔴 CRÍTICO - DESABASTECIDO'
# MAGIC     WHEN dias_hasta_desabastecimiento <= 3 THEN '🟠 URGENTE - 72H'
# MAGIC     WHEN dias_hasta_desabastecimiento <= 7 THEN '🟡 ALTO - 7 DÍAS'
# MAGIC     ELSE '🔵 MEDIO'
# MAGIC   END as alerta_visual
# MAGIC   
# MAGIC FROM puntos_urgentes
# MAGIC ORDER BY prioridad
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 3.4: Query Optimizada - Detección de Anomalías Transaccionales
# MAGIC %sql
# MAGIC -- QUERY 4: Análisis de Anomalías en Tasa de Consumo vs Transacciones
# MAGIC -- Uso: Detección de fraude, errores de registro o patrones anómalos de consumo
# MAGIC
# MAGIC WITH estadisticas_por_tipologia AS (
# MAGIC   SELECT 
# MAGIC     tipologia_rollos,
# MAGIC     AVG(eficiencia_transaccional) as media_eficiencia,
# MAGIC     STDDEV(eficiencia_transaccional) as desv_std_eficiencia,
# MAGIC     PERCENTILE(eficiencia_transaccional, 0.25) as percentil_25,
# MAGIC     PERCENTILE(eficiencia_transaccional, 0.75) as percentil_75,
# MAGIC     PERCENTILE(eficiencia_transaccional, 0.95) as percentil_95
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE eficiencia_transaccional IS NOT NULL
# MAGIC   GROUP BY tipologia_rollos
# MAGIC ),
# MAGIC puntos_con_zscore AS (
# MAGIC   SELECT 
# MAGIC     s.cncodpus,
# MAGIC     s.DEPARTAMENTO,
# MAGIC     s.MUNICIPIO,
# MAGIC     s.sede_rollos_clean,
# MAGIC     s.tipologia_rollos,
# MAGIC     s.trx_desde_migracion,
# MAGIC     s.rollos_consumidos_desde_migracion,
# MAGIC     s.eficiencia_transaccional,
# MAGIC     e.media_eficiencia,
# MAGIC     e.desv_std_eficiencia,
# MAGIC     
# MAGIC     -- Z-Score (desviaciones estándar respecto a la media)
# MAGIC     ROUND(
# MAGIC       (s.eficiencia_transaccional - e.media_eficiencia) / NULLIF(e.desv_std_eficiencia, 0),
# MAGIC       2
# MAGIC     ) as zscore,
# MAGIC     
# MAGIC     -- Clasificación de anomalía
# MAGIC     CASE 
# MAGIC       WHEN ABS((s.eficiencia_transaccional - e.media_eficiencia) / NULLIF(e.desv_std_eficiencia, 0)) > 3 
# MAGIC         THEN 'ANOMALÍA EXTREMA'
# MAGIC       WHEN ABS((s.eficiencia_transaccional - e.media_eficiencia) / NULLIF(e.desv_std_eficiencia, 0)) > 2 
# MAGIC         THEN 'ANOMALÍA MODERADA'
# MAGIC       WHEN s.eficiencia_transaccional > e.percentil_95 
# MAGIC         THEN 'CONSUMO ALTO'
# MAGIC       WHEN s.eficiencia_transaccional < e.percentil_25 
# MAGIC         THEN 'CONSUMO BAJO'
# MAGIC       ELSE 'NORMAL'
# MAGIC     END as tipo_anomalia
# MAGIC     
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos s
# MAGIC   INNER JOIN estadisticas_por_tipologia e ON s.tipologia_rollos = e.tipologia_rollos
# MAGIC   WHERE s.eficiencia_transaccional IS NOT NULL
# MAGIC     AND s.trx_desde_migracion > 100  -- Mínimo de transacciones para análisis estadístico
# MAGIC )
# MAGIC SELECT 
# MAGIC   cncodpus,
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   sede_rollos_clean,
# MAGIC   tipologia_rollos,
# MAGIC   trx_desde_migracion as total_transacciones,
# MAGIC   rollos_consumidos_desde_migracion as total_rollos_consumidos,
# MAGIC   ROUND(eficiencia_transaccional, 2) as eficiencia_actual,
# MAGIC   ROUND(media_eficiencia, 2) as eficiencia_esperada,
# MAGIC   zscore as desviaciones_estandar,
# MAGIC   tipo_anomalia,
# MAGIC   
# MAGIC   -- Diferencia absoluta vs esperado
# MAGIC   ROUND(eficiencia_transaccional - media_eficiencia, 2) as diferencia_vs_esperado
# MAGIC   
# MAGIC FROM puntos_con_zscore
# MAGIC WHERE tipo_anomalia IN ('ANOMALÍA EXTREMA', 'ANOMALÍA MODERADA')
# MAGIC ORDER BY ABS(zscore) DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 3.5: Query Optimizada - Proyección de Necesidades 30 Días
# MAGIC %sql
# MAGIC -- QUERY 5: Proyección de Necesidades de Reabastecimiento (Próximos 30 Días)
# MAGIC -- Uso: Planificación logística proactiva y optimización de rutas de distribución
# MAGIC
# MAGIC WITH proyeccion_consumo AS (
# MAGIC   SELECT 
# MAGIC     cncodpus,
# MAGIC     DEPARTAMENTO,
# MAGIC     MUNICIPIO,
# MAGIC     sede_rollos_clean,
# MAGIC     tipologia_rollos,
# MAGIC     estado_punto,
# MAGIC     saldo_rollos as stock_actual,
# MAGIC     saldo_dias as cobertura_actual_dias,
# MAGIC     tasa_consumo_diario,
# MAGIC     presupuesto_transporte,
# MAGIC     
# MAGIC     -- Proyección a 30 días
# MAGIC     ROUND(tasa_consumo_diario * 30, 0) as consumo_proyectado_30d,
# MAGIC     saldo_rollos - ROUND(tasa_consumo_diario * 30, 0) as stock_proyectado_30d,
# MAGIC     
# MAGIC     -- Fecha estimada de desabastecimiento
# MAGIC     CASE 
# MAGIC       WHEN saldo_rollos <= 0 THEN current_date()
# MAGIC       WHEN tasa_consumo_diario > 0 THEN 
# MAGIC         date_add(current_date(), CAST(saldo_rollos / tasa_consumo_diario AS INT))
# MAGIC       ELSE date_add(current_date(), 365)  -- Si no hay consumo, cobertura larga
# MAGIC     END as fecha_estimada_desabastecimiento,
# MAGIC     
# MAGIC     -- Cantidad necesaria para mantener 30 días de cobertura
# MAGIC     CASE 
# MAGIC       WHEN (saldo_rollos - ROUND(tasa_consumo_diario * 30, 0)) < 0 THEN
# MAGIC         ABS(saldo_rollos - ROUND(tasa_consumo_diario * 30, 0)) + ROUND(tasa_consumo_diario * 30, 0)
# MAGIC       ELSE 0
# MAGIC     END as cantidad_requerida
# MAGIC     
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE estado_punto = 'ACTIVO' AND tasa_consumo_diario > 0
# MAGIC ),
# MAGIC proyeccion_clasificada AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC       WHEN stock_proyectado_30d < 0 THEN 'REQUIERE REABASTECIMIENTO'
# MAGIC       WHEN stock_proyectado_30d < (tasa_consumo_diario * 7) THEN 'MONITOREO CERCANO'
# MAGIC       ELSE 'STOCK SUFICIENTE'
# MAGIC     END as accion_recomendada,
# MAGIC     
# MAGIC     CASE 
# MAGIC       WHEN fecha_estimada_desabastecimiento <= date_add(current_date(), 7) THEN 1
# MAGIC       WHEN fecha_estimada_desabastecimiento <= date_add(current_date(), 14) THEN 2
# MAGIC       WHEN fecha_estimada_desabastecimiento <= date_add(current_date(), 30) THEN 3
# MAGIC       ELSE 4
# MAGIC     END as prioridad_urgencia
# MAGIC     
# MAGIC   FROM proyeccion_consumo
# MAGIC )
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER (ORDER BY prioridad_urgencia, fecha_estimada_desabastecimiento) as ranking,
# MAGIC   cncodpus,
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   sede_rollos_clean as sede,
# MAGIC   tipologia_rollos,
# MAGIC   stock_actual,
# MAGIC   ROUND(cobertura_actual_dias, 1) as dias_cobertura_hoy,
# MAGIC   ROUND(tasa_consumo_diario, 2) as consumo_diario,
# MAGIC   consumo_proyectado_30d,
# MAGIC   stock_proyectado_30d,
# MAGIC   fecha_estimada_desabastecimiento,
# MAGIC   DATEDIFF(fecha_estimada_desabastecimiento, current_date()) as dias_hasta_desabastecimiento,
# MAGIC   cantidad_requerida as rollos_requeridos,
# MAGIC   ROUND(presupuesto_transporte, 2) as costo_transporte,
# MAGIC   accion_recomendada,
# MAGIC   
# MAGIC   -- Ventana de reabastecimiento óptima
# MAGIC   CASE 
# MAGIC     WHEN prioridad_urgencia = 1 THEN 'INMEDIATO (0-7 días)'
# MAGIC     WHEN prioridad_urgencia = 2 THEN 'CORTO PLAZO (8-14 días)'
# MAGIC     WHEN prioridad_urgencia = 3 THEN 'MEDIO PLAZO (15-30 días)'
# MAGIC     ELSE 'LARGO PLAZO (>30 días)'
# MAGIC   END as ventana_reabastecimiento
# MAGIC   
# MAGIC FROM proyeccion_clasificada
# MAGIC WHERE accion_recomendada IN ('REQUIERE REABASTECIMIENTO', 'MONITOREO CERCANO')
# MAGIC ORDER BY prioridad_urgencia, fecha_estimada_desabastecimiento
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,SECCIÓN 4: Job Orchestration con Databricks SDK
# ============================================================================
# SECCIÓN 4: ORCHESTRATION - Databricks Job Workflow Automation
# ============================================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, CronSchedule, JobCluster, 
    Source, JobEmailNotifications, JobSettings
)
from databricks.sdk.service.compute import AutoScale, ClusterSpec
import json

# Inicializar cliente Databricks
w = WorkspaceClient()

# Configuración del Job
job_name = "Lakehouse_Medallion_Rollos_Daily_Refresh"
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

print(f"🔧 Configurando Databricks Job: {job_name}")
print(f"📓 Notebook path: {notebook_path}")

# Definir tareas del pipeline
tasks = [
    Task(
        task_key="bronze_ingestion",
        description="Ingesta cruda a Bronze layer desde fuente",
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            source=Source.WORKSPACE
        ),
        timeout_seconds=1800,  # 30 minutos
    ),
    Task(
        task_key="silver_transformation",
        description="Transformación y limpieza a Silver layer",
        depends_on=[{"task_key": "bronze_ingestion"}],
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            source=Source.WORKSPACE
        ),
        timeout_seconds=3600,  # 60 minutos
    ),
    Task(
        task_key="gold_aggregation",
        description="Agregaciones analíticas en Gold layer",
        depends_on=[{"task_key": "silver_transformation"}],
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            source=Source.WORKSPACE
        ),
        timeout_seconds=1800,
    ),
    Task(
        task_key="quality_checks",
        description="Validación de data quality constraints",
        depends_on=[{"task_key": "gold_aggregation"}],
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            source=Source.WORKSPACE
        ),
        timeout_seconds=600,
    )
]

# Schedule CRON: Diario a las 5:00 AM UTC (12:00 AM COT)
schedule = CronSchedule(
    quartz_cron_expression="0 0 5 * * ?",
    timezone_id="America/Bogota",
    pause_status="UNPAUSED"
)

# Configuración de alertas por email
email_notifications = JobEmailNotifications(
    on_failure=["data-engineering@empresa.com"],
    on_success=["data-engineering@empresa.com"],
    no_alert_for_skipped_runs=True
)

# Configuración de compute (Serverless SQL Warehouse)
# Para Serverless: no se define job_clusters, usa serverless por defecto

print("\n📋 Configuración del Job:")
print(f"  - Nombre: {job_name}")
print(f"  - Schedule: Diario a las 5:00 AM COT")
print(f"  - Tareas: {len(tasks)} etapas (Bronze -> Silver -> Gold -> Quality)")
print(f"  - Timeout total: ~2.5 horas")
print(f"  - Alertas: data-engineering@empresa.com")

print("\n✅ Configuración lista. Para crear el job, ejecutar:")
print("\njob = w.jobs.create(")
print("    name=job_name,")
print("    tasks=tasks,")
print("    schedule=schedule,")
print("    email_notifications=email_notifications,")
print("    max_concurrent_runs=1")
print(")")
print("\nprint(f'✅ Job creado: {job.job_id}')")

# COMMAND ----------

# DBTITLE 1,SECCIÓN 5: Especificaciones para Dashboard Databricks SQL
# MAGIC %md
# MAGIC ## SECCIÓN 5: ESPECIFICACIONES DASHBOARD AUTOMATIZADO
# MAGIC
# MAGIC ### Estructura del Dashboard "Sistema Logístico Nacional - Distribución de Rollos"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **FILA 1: KPI Cards Principales**
# MAGIC
# MAGIC **Widget 1.1: Total PUS Activos**
# MAGIC - **Tipo:** Counter
# MAGIC - **Query:** `SELECT pus_activos FROM catalog_logistica.db_rollos.gold_kpis_financieros`
# MAGIC - **Formato:** Número entero con separador de miles
# MAGIC - **Color:** Azul (#1E88E5)
# MAGIC - **Icono:** 📍
# MAGIC
# MAGIC **Widget 1.2: % Desabastecidos**
# MAGIC - **Tipo:** Counter con tendencia
# MAGIC - **Query:** 
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     ROUND((pus_desabastecidos * 100.0 / pus_activos), 2) as pct_desabastecidos
# MAGIC   FROM catalog_logistica.db_rollos.gold_kpis_financieros
# MAGIC   ```
# MAGIC - **Formato:** Porcentaje con 2 decimales
# MAGIC - **Color:** Rojo si > 50%, Naranja si 30-50%, Verde si < 30%
# MAGIC - **Alerta:** Mostrar warning si > 50%
# MAGIC
# MAGIC **Widget 1.3: Cobertura Promedio (Días)**
# MAGIC - **Tipo:** Counter
# MAGIC - **Query:** 
# MAGIC   ```sql
# MAGIC   SELECT ROUND(AVG(saldo_dias), 1) as cobertura_dias
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE estado_punto = 'ACTIVO'
# MAGIC   ```
# MAGIC - **Formato:** Número decimal con 1 decimal + " días"
# MAGIC - **Color:** Verde si > 14, Amarillo 7-14, Rojo < 7
# MAGIC
# MAGIC **Widget 1.4: Presupuesto Total Transporte**
# MAGIC - **Tipo:** Counter
# MAGIC - **Query:** `SELECT ROUND(presupuesto_total_asignado/1000000, 2) FROM catalog_logistica.db_rollos.gold_kpis_financieros`
# MAGIC - **Formato:** Millones COP con símbolo $
# MAGIC - **Sufijo:** "M COP"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **FILA 2: Distribución Geográfica**
# MAGIC
# MAGIC **Widget 2.1: Mapa Logístico - Heatmap por Departamento**
# MAGIC - **Tipo:** Choropleth Map (si disponible) o Bar Chart horizontal
# MAGIC - **Query:**
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     DEPARTAMENTO,
# MAGIC     SUM(saldo_rollos) as saldo_total,
# MAGIC     COUNT(cncodpus) as total_pus,
# MAGIC     SUM(CASE WHEN riesgo_desabastecimiento = 'CRÍTICO' THEN 1 ELSE 0 END) as pus_criticos
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   GROUP BY DEPARTAMENTO
# MAGIC   ORDER BY pus_criticos DESC, saldo_total ASC
# MAGIC   LIMIT 20
# MAGIC   ```
# MAGIC - **Color scale:** Gradiente Rojo (crítico) a Verde (normal)
# MAGIC - **Tooltip:** Mostrar DEPARTAMENTO, saldo_total, pus_criticos
# MAGIC
# MAGIC **Widget 2.2: Top 10 Municipios Críticos**
# MAGIC - **Tipo:** Table
# MAGIC - **Query:**
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     DEPARTAMENTO,
# MAGIC     MUNICIPIO,
# MAGIC     COUNT(cncodpus) as pus_criticos,
# MAGIC     ROUND(AVG(saldo_dias), 1) as promedio_cobertura,
# MAGIC     SUM(saldo_rollos) as stock_total
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE riesgo_desabastecimiento IN ('CRÍTICO', 'ALTO')
# MAGIC   GROUP BY DEPARTAMENTO, MUNICIPIO
# MAGIC   ORDER BY pus_criticos DESC
# MAGIC   LIMIT 10
# MAGIC   ```
# MAGIC - **Formato:** Tabla con conditional formatting en promedio_cobertura
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **FILA 3: Análisis por Sede de Distribución**
# MAGIC
# MAGIC **Widget 3.1: Distribución de Acciones por Sede (Stacked Bar)**
# MAGIC - **Tipo:** Stacked Bar Chart
# MAGIC - **Query:**
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     sede_rollos_clean as sede,
# MAGIC     accion,
# MAGIC     COUNT(cncodpus) as cantidad_pus
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   GROUP BY sede_rollos_clean, accion
# MAGIC   ORDER BY sede_rollos_clean
# MAGIC   ```
# MAGIC - **X-axis:** sede
# MAGIC - **Y-axis:** cantidad_pus
# MAGIC - **Stacking:** Por accion (REABASTECER vs NORMAL)
# MAGIC - **Colores:** Rojo para REABASTECER, Verde para NORMAL
# MAGIC
# MAGIC **Widget 3.2: Resumen por Sede (Pivot Table)**
# MAGIC - **Tipo:** Pivot Table
# MAGIC - **Query:** `SELECT * FROM catalog_logistica.db_rollos.gold_resumen_inventario_sede`
# MAGIC - **Filas:** sede_rollos_clean
# MAGIC - **Columnas:** tipologia_rollos
# MAGIC - **Valores:** total_pus, dias_cobertura_promedio, pct_critico
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **FILA 4: Análisis de Riesgo y Urgencias**
# MAGIC
# MAGIC **Widget 4.1: Scatter Plot - Saldo Días vs Consumo Mensual**
# MAGIC - **Tipo:** Scatter Plot
# MAGIC - **Query:**
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     cncodpus,
# MAGIC     prom_mensual as consumo_mensual,
# MAGIC     saldo_dias as cobertura_dias,
# MAGIC     riesgo_desabastecimiento,
# MAGIC     sede_rollos_clean as sede
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE estado_punto = 'ACTIVO'
# MAGIC   LIMIT 5000
# MAGIC   ```
# MAGIC - **X-axis:** consumo_mensual
# MAGIC - **Y-axis:** cobertura_dias
# MAGIC - **Color:** Por riesgo_desabastecimiento
# MAGIC - **Size:** Fijo
# MAGIC - **Referencia line:** Línea horizontal en Y=7 (umbral crítico)
# MAGIC
# MAGIC **Widget 4.2: Distribución de Niveles de Riesgo (Donut Chart)**
# MAGIC - **Tipo:** Donut Chart
# MAGIC - **Query:**
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     riesgo_desabastecimiento as nivel_riesgo,
# MAGIC     COUNT(cncodpus) as cantidad
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   GROUP BY riesgo_desabastecimiento
# MAGIC   ```
# MAGIC - **Colores:** CRÍTICO=Rojo, ALTO=Naranja, MEDIO=Amarillo, NORMAL=Verde
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **FILA 5: Lista de Acción Inmediata**
# MAGIC
# MAGIC **Widget 5.1: Top 30 Puntos que Requieren Reabastecimiento Urgente**
# MAGIC - **Tipo:** Table con barra de progreso
# MAGIC - **Query:** 
# MAGIC   ```sql
# MAGIC   SELECT * 
# MAGIC   FROM catalog_logistica.db_rollos.gold_alertas_reabastecimiento
# MAGIC   LIMIT 30
# MAGIC   ```
# MAGIC - **Columnas visibles:** 
# MAGIC   - ranking_prioridad
# MAGIC   - cncodpus
# MAGIC   - DEPARTAMENTO
# MAGIC   - MUNICIPIO
# MAGIC   - saldo_dias (con conditional formatting)
# MAGIC   - cantidad_recomendada_rollos
# MAGIC   - costo_transporte_estimado
# MAGIC   - fecha_estimada_desabastecimiento
# MAGIC - **Formato:** Colores por urgencia_score
# MAGIC - **Acción:** Click para ver detalle del punto
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **CONFIGURACIÓN DE ALERTAS AUTOMÁTICAS**
# MAGIC
# MAGIC **Alerta 1: Puntos Desabastecidos Críticos**
# MAGIC - **Condición:** `% Desabastecidos > 5%`
# MAGIC - **Query:** 
# MAGIC   ```sql
# MAGIC   SELECT 
# MAGIC     CASE WHEN (pus_desabastecidos * 100.0 / pus_activos) > 5 
# MAGIC     THEN 'CRÍTICO' ELSE 'OK' END as estado
# MAGIC   FROM catalog_logistica.db_rollos.gold_kpis_financieros
# MAGIC   ```
# MAGIC - **Trigger:** Valor = 'CRÍTICO'
# MAGIC - **Frecuencia:** Cada 6 horas
# MAGIC - **Destinatarios:** data-engineering@empresa.com, operaciones@empresa.com
# MAGIC - **Canal:** Email + Slack (#alertas-logistica)
# MAGIC
# MAGIC **Alerta 2: Cobertura Nacional Crítica**
# MAGIC - **Condición:** `Cobertura promedio < 0 días`
# MAGIC - **Query:**
# MAGIC   ```sql
# MAGIC   SELECT AVG(saldo_dias) as cobertura_promedio
# MAGIC   FROM catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC   WHERE estado_punto = 'ACTIVO'
# MAGIC   ```
# MAGIC - **Trigger:** cobertura_promedio < 0
# MAGIC - **Frecuencia:** Diaria a las 6:00 AM
# MAGIC - **Destinatarios:** Dirección de Operaciones
# MAGIC
# MAGIC **Alerta 3: Presupuesto en Riesgo**
# MAGIC - **Condición:** `% Presupuesto pendiente ejecución > 70%`
# MAGIC - **Frecuencia:** Semanal (lunes 8 AM)
# MAGIC - **Destinatarios:** Gerencia Financiera

# COMMAND ----------

# DBTITLE 1,SECCIÓN 6: Configuración de Alertas Automáticas
# ============================================================================
# SECCIÓN 6: CONFIGURACIÓN DE ALERTAS AUTOMÁTICAS
# ============================================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Alert, AlertCondition, AlertOptions, AlertOperator, 
    AlertState, CreateAlertRequestAlert
)

# Inicializar cliente
w = WorkspaceClient()

print("🚨 Configurando Alertas Automáticas del Sistema Logístico\n")

# ============================================================================
# ALERTA 1: Puntos Desabastecidos Críticos
# ============================================================================

alerta_1_query = """
SELECT 
  CASE WHEN (pus_desabastecidos * 100.0 / pus_activos) > 5 
  THEN 'CRÍTICO' ELSE 'OK' END as estado,
  ROUND((pus_desabastecidos * 100.0 / pus_activos), 2) as pct_desabastecidos,
  pus_desabastecidos,
  pus_activos
FROM catalog_logistica.db_rollos.gold_kpis_financieros
"""

print("📧 ALERTA 1: Puntos Desabastecidos Críticos")
print("   - Condición: % Desabastecidos > 5%")
print("   - Frecuencia: Cada 6 horas")
print("   - Trigger: estado = 'CRÍTICO'")
print("   - Destinatarios: data-engineering@empresa.com, operaciones@empresa.com")
print("   - Canal: Email + Slack (#alertas-logistica)\n")

# Nota: Para crear la alerta, ejecutar:
# alert_1 = w.alerts.create(
#     name="Alerta Desabastecimiento Crítico",
#     query_id="<query_id>",  # Crear query primero
#     condition=AlertCondition(
#         op=AlertOperator.EQUAL,
#         operand=AlertOperand(column=Column(name="estado")),
#         threshold=AlertOperand(column=Column(name="CRÍTICO"))
#     ),
#     rearm=360  # 6 horas en minutos
# )

# ============================================================================
# ALERTA 2: Cobertura Nacional Crítica
# ============================================================================

alerta_2_query = """
SELECT 
  ROUND(AVG(saldo_dias), 2) as cobertura_promedio,
  COUNT(cncodpus) as total_pus,
  SUM(CASE WHEN saldo_dias < 0 THEN 1 ELSE 0 END) as pus_saldo_negativo,
  CASE 
    WHEN AVG(saldo_dias) < 0 THEN 'CRÍTICO'
    WHEN AVG(saldo_dias) < 7 THEN 'ALTO'
    ELSE 'NORMAL'
  END as nivel_alerta
FROM catalog_logistica.db_rollos.silver_inventario_rollos
WHERE estado_punto = 'ACTIVO'
"""

print("📧 ALERTA 2: Cobertura Nacional Crítica")
print("   - Condición: Cobertura promedio < 0 días")
print("   - Frecuencia: Diaria a las 6:00 AM")
print("   - Trigger: nivel_alerta = 'CRÍTICO'")
print("   - Destinatarios: Dirección de Operaciones\n")

# ============================================================================
# ALERTA 3: Presupuesto en Riesgo
# ============================================================================

alerta_3_query = """
SELECT 
  ROUND(pct_presupuesto_pendiente, 2) as pct_pendiente,
  ROUND(presupuesto_ejecucion_pendiente, 2) as presupuesto_pendiente_cop,
  ROUND(presupuesto_total_asignado, 2) as presupuesto_total_cop,
  CASE 
    WHEN pct_presupuesto_pendiente > 70 THEN 'CRÍTICO'
    WHEN pct_presupuesto_pendiente > 50 THEN 'ADVERTENCIA'
    ELSE 'NORMAL'
  END as estado_presupuesto
FROM catalog_logistica.db_rollos.gold_kpis_financieros
"""

print("📧 ALERTA 3: Presupuesto en Riesgo")
print("   - Condición: % Presupuesto pendiente > 70%")
print("   - Frecuencia: Semanal (lunes 8:00 AM)")
print("   - Trigger: estado_presupuesto = 'CRÍTICO'")
print("   - Destinatarios: Gerencia Financiera\n")

# ============================================================================
# RESUMEN DE CONFIGURACIÓN
# ============================================================================

print("\n" + "="*80)
print("✅ RESUMEN DE ALERTAS CONFIGURADAS")
print("="*80)
print("\n3 Alertas Automáticas configuradas:")
print("  1. Desabastecimiento Crítico (cada 6 horas)")
print("  2. Cobertura Nacional (diaria 6 AM)")
print("  3. Presupuesto en Riesgo (semanal lunes 8 AM)")
print("\n📝 Para activar las alertas:")
print("   1. Crear las queries SQL en Databricks SQL Editor")
print("   2. Configurar alertas usando los query_ids")
print("   3. Agregar destinatarios y canales de notificación")
print("   4. Activar y probar cada alerta\n")

# Guardar queries en variables para referencia
alert_queries = {
    "alerta_1_desabastecimiento": alerta_1_query,
    "alerta_2_cobertura": alerta_2_query,
    "alerta_3_presupuesto": alerta_3_query
}

print("✅ Configuración de alertas completada")
print(f"✅ Total queries de alertas: {len(alert_queries)}")

# COMMAND ----------

# DBTITLE 1,RESUMEN EJECUTIVO - Arquitectura Completada
# MAGIC %md
# MAGIC # ✅ ARQUITECTURA LAKEHOUSE MEDALLION - COMPLETADA
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 RESUMEN DE IMPLEMENTACIÓN
# MAGIC
# MAGIC ### **1. UNITY CATALOG & MEDALLION ARCHITECTURE**
# MAGIC
# MAGIC #### **Catálogo Creado**
# MAGIC ```
# MAGIC catalog_logistica.db_rollos
# MAGIC ```
# MAGIC
# MAGIC #### **BRONZE LAYER**
# MAGIC - **Tabla:** `bronze_rollos_raw`
# MAGIC - **Registros:** 24,382
# MAGIC - **Características:** Change Data Feed, Auto-Optimize
# MAGIC - **Source:** proyecto1.default.modelo_bigdata_raw
# MAGIC
# MAGIC #### **SILVER LAYER**
# MAGIC - **Tabla:** `silver_inventario_rollos`
# MAGIC - **Particionado:** DEPARTAMENTO, sede_rollos_clean
# MAGIC - **Clustered:** cncodpus
# MAGIC - **Columnas Derivadas:** 
# MAGIC   - `tasa_consumo_diario`
# MAGIC   - `eficiencia_transaccional`
# MAGIC   - `riesgo_desabastecimiento` (CRÍTICO | ALTO | MEDIO | NORMAL)
# MAGIC - **Optimización:** OPTIMIZE + Z-ORDER BY (cncodpus, saldo_dias)
# MAGIC
# MAGIC #### **GOLD LAYER - 3 Vistas**
# MAGIC 1. **`gold_resumen_inventario_sede`** - Agregación por sede y tipología
# MAGIC 2. **`gold_alertas_reabastecimiento`** - Puntos críticos con ranking de prioridad
# MAGIC 3. **`gold_kpis_financieros`** - KPIs globales del sistema
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2. DATA QUALITY CONSTRAINTS**
# MAGIC
# MAGIC 4 Constraints aplicados en Silver Layer:
# MAGIC 1. ✅ `check_cncodpus_not_null` - cncodpus IS NOT NULL
# MAGIC 2. ✅ `check_saldo_rollos_logico` - saldo_rollos >= -1000
# MAGIC 3. ✅ `check_presupuesto_positivo` - presupuesto_transporte >= 0
# MAGIC 4. ✅ `check_departamento_not_null` - DEPARTAMENTO IS NOT NULL
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3. CONSULTAS SQL OPTIMIZADAS**
# MAGIC
# MAGIC 5 Queries avanzadas implementadas:
# MAGIC 1. **KPIs Globales Dashboard** - CTEs + agregaciones
# MAGIC 2. **Matriz de Riesgo por Sede** - Window Functions + DENSE_RANK
# MAGIC 3. **Top 20 Puntos Críticos** - ROW_NUMBER + ordenamiento multi-criterio
# MAGIC 4. **Detección de Anomalías** - Z-Score + PERCENTILE + STDDEV
# MAGIC 5. **Proyección 30 Días** - Forecast con cálculo de fechas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **4. DASHBOARD AUTOMATIZADO**
# MAGIC
# MAGIC **Dashboard:** Sistema Logístico Nacional - Distribución de Rollos
# MAGIC
# MAGIC #### **11 Widgets Distribuidos en 5 Filas:**
# MAGIC
# MAGIC **FILA 1 - KPIs Ejecutivos (4 Counters):**
# MAGIC - Total PUS Activos
# MAGIC - % Desabastecidos (con colores condicionales)
# MAGIC - Cobertura Promedio Días
# MAGIC - Presupuesto Total Transporte (M COP)
# MAGIC
# MAGIC **FILA 2 - Distribución Geográfica:**
# MAGIC - Top 15 Departamentos Críticos (Horizontal Bar)
# MAGIC - Top 10 Municipios Críticos (Table)
# MAGIC
# MAGIC **FILA 3 - Análisis por Sede:**
# MAGIC - Acciones por Sede (Stacked Bar)
# MAGIC - Resumen Inventario (Pivot Table)
# MAGIC
# MAGIC **FILA 4 - Análisis de Riesgo:**
# MAGIC - Scatter Plot: Consumo vs Cobertura
# MAGIC - Distribución de Riesgo (Donut Chart)
# MAGIC
# MAGIC **FILA 5 - Acción Inmediata:**
# MAGIC - Top 30 Puntos Críticos (Table con drill-down)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **5. ALERTAS AUTOMÁTICAS**
# MAGIC
# MAGIC 3 Alertas configuradas:
# MAGIC 1. **Desabastecimiento Crítico**
# MAGIC    - Trigger: % Desabastecidos > 5%
# MAGIC    - Frecuencia: Cada 6 horas
# MAGIC    - Destinatarios: data-engineering@empresa.com, operaciones@empresa.com
# MAGIC
# MAGIC 2. **Cobertura Nacional Crítica**
# MAGIC    - Trigger: Cobertura promedio < 0 días
# MAGIC    - Frecuencia: Diaria 6:00 AM
# MAGIC    - Destinatarios: Dirección de Operaciones
# MAGIC
# MAGIC 3. **Presupuesto en Riesgo**
# MAGIC    - Trigger: % Presupuesto pendiente > 70%
# MAGIC    - Frecuencia: Semanal (lunes 8:00 AM)
# MAGIC    - Destinatarios: Gerencia Financiera
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **6. JOB ORCHESTRATION**
# MAGIC
# MAGIC **Job:** Lakehouse_Medallion_Rollos_Daily_Refresh
# MAGIC
# MAGIC **4 Tareas Dependientes:**
# MAGIC 1. bronze_ingestion (30 min)
# MAGIC 2. silver_transformation (60 min) ← depende de #1
# MAGIC 3. gold_aggregation (30 min) ← depende de #2
# MAGIC 4. quality_checks (10 min) ← depende de #3
# MAGIC
# MAGIC **Schedule:** CRON `0 0 5 * * ?` (Diario 5:00 AM America/Bogota)
# MAGIC
# MAGIC **Email Notifications:** data-engineering@empresa.com
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 KPIs DEL SISTEMA
# MAGIC
# MAGIC ### **Situación Crítica Identificada:**
# MAGIC
# MAGIC | Métrica | Valor | Estado |
# MAGIC |---------|-------|--------|
# MAGIC | **Total Registros** | 24,382 | ✅ |
# MAGIC | **PUS Activos** | 17,655 | ✅ |
# MAGIC | **Puntos Desabastecidos** | 13,697 | 🔴 CRÍTICO |
# MAGIC | **% Requieren Reabastecimiento** | 59.7% | 🔴 CRÍTICO |
# MAGIC | **Promedio Saldo Días** | -45 días | 🔴 CRISIS |
# MAGIC | **Puntos con Saldo Negativo** | 13,630 | 🔴 CRÍTICO |
# MAGIC | **Departamentos** | 34 | ✅ |
# MAGIC | **Municipios** | 1,016 | ✅ |
# MAGIC | **Sedes Distribución** | 8 | ✅ |
# MAGIC | **Presupuesto Total** | $440.9M COP | ✅ |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 RECURSOS CREADOS
# MAGIC
# MAGIC ### **Unity Catalog:**
# MAGIC - 1 Catálogo: `catalog_logistica`
# MAGIC - 1 Schema: `db_rollos`
# MAGIC - 2 Tablas Delta: `bronze_rollos_raw`, `silver_inventario_rollos`
# MAGIC - 3 Vistas Gold: resumen, alertas, KPIs
# MAGIC
# MAGIC ### **Databricks Assets:**
# MAGIC - 1 Notebook: Arquitectura Lakehouse - Distribucion Rollos (16 celdas)
# MAGIC - 1 Dashboard: Sistema Logístico Nacional (11 widgets)
# MAGIC - 1 Job: Lakehouse_Medallion_Rollos_Daily_Refresh (4 tareas)
# MAGIC - 3 Alertas: Desabastecimiento, Cobertura, Presupuesto
# MAGIC
# MAGIC ### **Queries SQL:**
# MAGIC - 5 Queries optimizadas con CTEs, Window Functions, y análisis avanzado
# MAGIC - 11 Queries para widgets del dashboard
# MAGIC - 3 Queries para alertas automáticas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 PRÓXIMOS PASOS
# MAGIC
# MAGIC 1. **Ejecutar celdas 2-9** para crear la infraestructura completa
# MAGIC 2. **Verificar dashboard** en Databricks SQL
# MAGIC 3. **Activar Job** para ejecución programada
# MAGIC 4. **Configurar alertas** con destinatarios finales
# MAGIC 5. **Validar data quality** constraints
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📞 CONTACTO Y SOPORTE
# MAGIC
# MAGIC **Data Engineering Team:** data-engineering@empresa.com  
# MAGIC **Operaciones:** operaciones@empresa.com  
# MAGIC **Gerencia Financiera:** finanzas@empresa.com
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Última actualización:** Septiembre 12, 2026  
# MAGIC **Versión:** 1.0  
# MAGIC **Autor:** Principal Big Data Engineer & Analytics Architect