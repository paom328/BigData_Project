# Databricks notebook source
# DBTITLE 1,Documentación Completa DDL
# MAGIC %md
# MAGIC # 📜 DDL COMPLETO - ARQUITECTURA LAKEHOUSE MEDALLION
# MAGIC ## Sistema Logístico Nacional - Distribución de Rollos
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Arquitecto:** Principal Big Data Engineer & Analytics Architect  
# MAGIC **Fecha:** Septiembre 12, 2026  
# MAGIC **Plataforma:** Databricks Unity Catalog  
# MAGIC **Patrón:** Medallion Architecture (Bronze → Silver → Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 DATOS DEL SISTEMA
# MAGIC
# MAGIC - **Total Registros:** 24,382
# MAGIC - **Puntos de Servicio (PUS):** 24,380 únicos
# MAGIC - **Cobertura Geográfica:** 34 departamentos, 1,016 municipios
# MAGIC - **Sedes de Distribución:** 8
# MAGIC - **Presupuesto Total:** $440,933,125 COP
# MAGIC
# MAGIC ### **Situación Crítica:**
# MAGIC - 🔴 **59.7%** de puntos requieren reabastecimiento (14,556 PUS)
# MAGIC - 🔴 **13,697 puntos desabastecidos**
# MAGIC - 🔴 **Promedio saldo: -45 días** (crisis de stock nacional)
# MAGIC - 🔴 **13,630 puntos con saldo negativo**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 ÍNDICE DE CONTENIDO
# MAGIC
# MAGIC 1. **Creación de Catálogo y Schemas**
# MAGIC 2. **Bronze Layer DDL**
# MAGIC 3. **Silver Layer DDL**
# MAGIC 4. **Gold Layer - 3 Vistas**
# MAGIC 5. **Data Quality Constraints**
# MAGIC 6. **5 Queries SQL Optimizadas**
# MAGIC 7. **Configuración de Job Workflow**
# MAGIC 8. **Especificaciones de Dashboard**
# MAGIC 9. **Alertas Automáticas**
# MAGIC 10. **Comando de Ejecución Completa**

# COMMAND ----------

# DBTITLE 1,1. Creación de Catálogo y Schemas
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- PASO 1: CREACIÓN DE CATÁLOGO Y SCHEMAS UNITY CATALOG
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Crear catálogo para el sistema logístico
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_logistica
# MAGIC COMMENT 'Catálogo Unity Catalog para gestión logística de distribución de rollos';
# MAGIC
# MAGIC -- Usar el catálogo
# MAGIC USE CATALOG catalog_logistica;
# MAGIC
# MAGIC -- Crear schema para datos de rollos
# MAGIC CREATE SCHEMA IF NOT EXISTS db_rollos
# MAGIC COMMENT 'Schema Medallion Architecture: Bronze -> Silver -> Gold layers';
# MAGIC
# MAGIC -- Usar el schema
# MAGIC USE SCHEMA db_rollos;
# MAGIC
# MAGIC -- Verificar creación
# MAGIC SHOW SCHEMAS IN catalog_logistica;

# COMMAND ----------

# DBTITLE 1,2. Bronze Layer DDL
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- BRONZE LAYER: Capa de ingesta cruda (Raw Data)
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Nota: Ejecutar solo si necesitas recrear la tabla
# MAGIC -- DROP TABLE IF EXISTS catalog_logistica.db_rollos.bronze_rollos_raw;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS catalog_logistica.db_rollos.bronze_rollos_raw
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 7 days',
# MAGIC   'delta.logRetentionDuration' = 'interval 30 days'
# MAGIC )
# MAGIC COMMENT 'Bronze Layer: Datos crudos de distribución de rollos sin transformación'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS _ingestion_timestamp,
# MAGIC   'proyecto1.default.modelo_bigdata_raw' AS _source_table,
# MAGIC   input_file_name() AS _source_file
# MAGIC FROM proyecto1.default.modelo_bigdata_raw;
# MAGIC
# MAGIC -- Estadísticas de ingesta
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_registros_bronze,
# MAGIC   COUNT(DISTINCT cncodpus) as puntos_unicos,
# MAGIC   MIN(_ingestion_timestamp) as primera_ingesta,
# MAGIC   MAX(_ingestion_timestamp) as ultima_ingesta,
# MAGIC   COUNT(DISTINCT _source_table) as total_fuentes
# MAGIC FROM catalog_logistica.db_rollos.bronze_rollos_raw;
# MAGIC
# MAGIC -- Descripción de la tabla
# MAGIC DESCRIBE EXTENDED catalog_logistica.db_rollos.bronze_rollos_raw;

# COMMAND ----------

# DBTITLE 1,3. Silver Layer DDL - Transformación Completa
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- SILVER LAYER: Datos limpios, transformados y optimizados
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Nota: Ejecutar solo si necesitas recrear la tabla
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
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 7 days',
# MAGIC   'delta.logRetentionDuration' = 'interval 30 days',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC )
# MAGIC COMMENT 'Silver Layer: Datos transformados con tipos correctos, particionado y limpieza'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- ========================================================================
# MAGIC   -- IDENTIFICADORES
# MAGIC   -- ========================================================================
# MAGIC   cncodpus,
# MAGIC   `ESTADO PUNTO` as estado_punto,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- MÉTRICAS DE CONSUMO (LIMPIAS)
# MAGIC   -- ========================================================================
# MAGIC   prom_mensual_anterior,
# MAGIC   rango_prom_anterior,
# MAGIC   prom_mensual,
# MAGIC   rango_prom,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- UBICACIÓN GEOGRÁFICA
# MAGIC   -- ========================================================================
# MAGIC   `DEPARTAMENTO - MUNICIPIO` as departamento_municipio,
# MAGIC   DEPARTAMENTO,
# MAGIC   MUNICIPIO,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- SEDES Y TIPOLOGÍAS
# MAGIC   -- ========================================================================
# MAGIC   `SEDE OPERACIONES` as sede_operaciones,
# MAGIC   `TIPOLOGIA OPERACIONES` as tipologia_operaciones,
# MAGIC   `SEDE ROLLOS` as sede_rollos_clean,
# MAGIC   `TIPOLOGIA ROLLOS` as tipologia_rollos,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- DIMENSIONES DE CARGA
# MAGIC   -- ========================================================================
# MAGIC   `Q MUNIC` as q_municipio,
# MAGIC   `Q CB X MUNIC` as q_cb_por_municipio,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- FINANCIERO (CAST A TIPOS CORRECTOS)
# MAGIC   -- ========================================================================
# MAGIC   CAST(`PPTO TRANSP` AS DECIMAL(18,2)) as presupuesto_transporte,
# MAGIC   CAST(`PPTO PUNTO TRANSP MUNIC` AS DECIMAL(18,2)) as presupuesto_punto_transporte_municipio,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- MÉTRICAS DE ABASTECIMIENTO E-5
# MAGIC   -- ========================================================================
# MAGIC   `PERIODO ABAST E-5` as periodo_abastecimiento_e5,
# MAGIC   `ROLLOS PERIODO ABAST E-5` as rollos_periodo_abast_e5,
# MAGIC   `ROLLOS AÑO E-5` as rollos_anio_e5,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- FECHAS (PARSE CORRECTO)
# MAGIC   -- ========================================================================
# MAGIC   TO_DATE(`FECHA MIGRACIÓN O APERTURA`, 'M/d/yy') as fecha_migracion_apertura,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- MÉTRICAS OPERATIVAS (CAST SEGURO A TIPOS NUMÉRICOS)
# MAGIC   -- ========================================================================
# MAGIC   TRY_CAST(`ROLLOS ENTREGADOS DESDE MIGRACIÓN O APERTURA` AS BIGINT) as rollos_entregados_desde_migracion,
# MAGIC   TRY_CAST(`TRX DESDE MIGRACIÓN O APERTURA` AS BIGINT) as trx_desde_migracion,
# MAGIC   TRY_CAST(`ROLLOS CONSUMIDOS DESDE MIGRACIÓN O APERTURA` AS BIGINT) as rollos_consumidos_desde_migracion,
# MAGIC   TRY_CAST(`SALDO ROLLOS` AS INT) as saldo_rollos,
# MAGIC   TRY_CAST(`SALDO DIAS` AS DOUBLE) as saldo_dias,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- ALERTAS Y ACCIONES
# MAGIC   -- ========================================================================
# MAGIC   `ACCIÓN` as accion,
# MAGIC   T as estado_desabastecimiento,
# MAGIC   TRY_CAST(`FECHA_PLAN_ABAST_1` AS INT) as fecha_plan_abastecimiento_epoch,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- COLUMNAS DERIVADAS (BUSINESS LOGIC)
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
# MAGIC   -- Días hasta desabastecimiento total
# MAGIC   CASE 
# MAGIC     WHEN TRY_CAST(`SALDO DIAS` AS DOUBLE) < 0 THEN 0
# MAGIC     ELSE CAST(TRY_CAST(`SALDO DIAS` AS DOUBLE) AS INT)
# MAGIC   END as dias_hasta_desabastecimiento,
# MAGIC   
# MAGIC   -- ========================================================================
# MAGIC   -- METADATA
# MAGIC   -- ========================================================================
# MAGIC   current_timestamp() as _processed_timestamp,
# MAGIC   current_date() as _partition_date
# MAGIC   
# MAGIC FROM catalog_logistica.db_rollos.bronze_rollos_raw
# MAGIC WHERE cncodpus IS NOT NULL;
# MAGIC
# MAGIC -- Optimizar tabla (OPTIMIZE y Z-ORDER)
# MAGIC OPTIMIZE catalog_logistica.db_rollos.silver_inventario_rollos
# MAGIC ZORDER BY (cncodpus, saldo_dias);
# MAGIC
# MAGIC -- Estadísticas Silver
# MAGIC ANALYZE TABLE catalog_logistica.db_rollos.silver_inventario_rollos COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

