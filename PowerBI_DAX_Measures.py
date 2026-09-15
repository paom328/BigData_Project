# Databricks notebook source
# DBTITLE 1,Power BI DAX Measures - Executive Dashboard
# MAGIC %md
# MAGIC # Power BI DAX Measures - Executive Dashboard
# MAGIC ## Paper Roll Logistics Control Tower
# MAGIC
# MAGIC **Purpose:** Production-ready DAX measures for the 3-page executive dashboard
# MAGIC
# MAGIC **Data Source:** DirectQuery to `catalog_logistica.db_rollos.gold_plan_abastecimiento`
# MAGIC
# MAGIC **Measure Categories:**
# MAGIC 1. Executive KPIs (Page 1)
# MAGIC 2. Coverage Analytics (Page 2)
# MAGIC 3. Operational Metrics (Page 3)
# MAGIC 4. Dynamic Calculations
# MAGIC 5. Conditional Formatting Logic
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,1. Executive KPI Measures (Page 1 - Control Tower)
# MAGIC %md
# MAGIC ## 1. Executive KPI Measures (Page 1 - Control Tower)
# MAGIC
# MAGIC ### Total Service Points (PUS)
# MAGIC ```dax
# MAGIC Total_PUS = 
# MAGIC COUNTROWS('gold_plan_abastecimiento')
# MAGIC ```
# MAGIC
# MAGIC ### Network Health Index %
# MAGIC ```dax
# MAGIC Network_Health_Index = 
# MAGIC VAR TotalSites = COUNTROWS('gold_plan_abastecimiento')
# MAGIC VAR HealthySites = 
# MAGIC     CALCULATE(
# MAGIC         COUNTROWS('gold_plan_abastecimiento'),
# MAGIC         'gold_plan_abastecimiento'[ESTADO_INVENTARIO] = "ABASTECIDO"
# MAGIC     )
# MAGIC RETURN
# MAGIC DIVIDE(HealthySites, TotalSites, 0) * 100
# MAGIC ```
# MAGIC
# MAGIC ### Critical Stock Locations (< 30 rolls)
# MAGIC ```dax
# MAGIC Critical_Stock_Count = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     'gold_plan_abastecimiento'[ESTADO_INVENTARIO] = "CRÍTICO"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Confirmed Stockouts
# MAGIC ```dax
# MAGIC Stockout_Count = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     'gold_plan_abastecimiento'[ESTADO_INVENTARIO] = "DESABASTECIDO"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Total Logistics Budget Executed
# MAGIC ```dax
# MAGIC Total_Freight_Budget = 
# MAGIC SUM('gold_plan_abastecimiento'[PPTO_TRANSP])
# MAGIC ```
# MAGIC
# MAGIC ### Critical Stockout % (for KPI visual)
# MAGIC ```dax
# MAGIC Critical_Stockout_Pct = 
# MAGIC VAR TotalSites = COUNTROWS('gold_plan_abastecimiento')
# MAGIC VAR CriticalSites = [Stockout_Count] + [Critical_Stock_Count]
# MAGIC RETURN
# MAGIC DIVIDE(CriticalSites, TotalSites, 0) * 100
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,2. Coverage Analytics Measures (Page 2)
# MAGIC %md
# MAGIC ## 2. Coverage Analytics Measures (Page 2)
# MAGIC
# MAGIC ### Average Coverage Days
# MAGIC ```dax
# MAGIC Avg_Coverage_Days = 
# MAGIC AVERAGE('gold_plan_abastecimiento'[SALDO_DIAS])
# MAGIC ```
# MAGIC
# MAGIC ### Sites by Coverage Bracket
# MAGIC ```dax
# MAGIC Sites_By_Bracket = 
# MAGIC COUNTROWS('gold_plan_abastecimiento')
# MAGIC ```
# MAGIC
# MAGIC ### Total Roll Balance (Network-wide)
# MAGIC ```dax
# MAGIC Total_Roll_Balance = 
# MAGIC SUM('gold_plan_abastecimiento'[SALDO_ROLLOS])
# MAGIC ```
# MAGIC
# MAGIC ### Average Transaction Volume
# MAGIC ```dax
# MAGIC Avg_Transaction_Volume = 
# MAGIC AVERAGE('gold_plan_abastecimiento'[PROM_TRANSACCIONES])
# MAGIC ```
# MAGIC
# MAGIC ### Sites Below 10-Day Coverage
# MAGIC ```dax
# MAGIC Sites_Below_10Days = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     'gold_plan_abastecimiento'[SALDO_DIAS] < 10
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Coverage Distribution Score
# MAGIC ```dax
# MAGIC Coverage_Distribution_Score = 
# MAGIC VAR Below5Days = 
# MAGIC     CALCULATE(
# MAGIC         COUNTROWS('gold_plan_abastecimiento'),
# MAGIC         'gold_plan_abastecimiento'[SALDO_DIAS] <= 5
# MAGIC     )
# MAGIC VAR Below10Days = 
# MAGIC     CALCULATE(
# MAGIC         COUNTROWS('gold_plan_abastecimiento'),
# MAGIC         'gold_plan_abastecimiento'[SALDO_DIAS] <= 10
# MAGIC     )
# MAGIC VAR TotalSites = COUNTROWS('gold_plan_abastecimiento')
# MAGIC RETURN
# MAGIC 100 - ((Below5Days * 2 + Below10Days) / TotalSites * 50)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,3. Operational Metrics (Page 3 - Dispatch Queue)
# MAGIC %md
# MAGIC ## 3. Operational Metrics (Page 3 - Dispatch Queue)
# MAGIC
# MAGIC ### Urgent Dispatch Count
# MAGIC ```dax
# MAGIC Urgent_Dispatch_Count = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     OR(
# MAGIC         'gold_plan_abastecimiento'[ESTADO_INVENTARIO] = "DESABASTECIDO",
# MAGIC         'gold_plan_abastecimiento'[SALDO_DIAS] <= 5
# MAGIC     )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### High-Volume Sites at Risk
# MAGIC ```dax
# MAGIC High_Volume_At_Risk = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     'gold_plan_abastecimiento'[PROM_TRANSACCIONES] > 1000,
# MAGIC     'gold_plan_abastecimiento'[SALDO_DIAS] < 15
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Dispatch Priority Score
# MAGIC ```dax
# MAGIC Dispatch_Priority_Score = 
# MAGIC VAR CoverageDays = 'gold_plan_abastecimiento'[SALDO_DIAS]
# MAGIC VAR TransactionVolume = 'gold_plan_abastecimiento'[PROM_TRANSACCIONES]
# MAGIC VAR HasPO = 'gold_plan_abastecimiento'[has_purchase_order]
# MAGIC RETURN
# MAGIC SWITCH(
# MAGIC     TRUE(),
# MAGIC     CoverageDays <= 0, 100,
# MAGIC     HasPO = TRUE(), 95,
# MAGIC     CoverageDays <= 5, 90,
# MAGIC     CoverageDays <= 10 && TransactionVolume > 1000, 80,
# MAGIC     CoverageDays <= 10, 70,
# MAGIC     CoverageDays <= 15, 50,
# MAGIC     CoverageDays <= 20, 30,
# MAGIC     10
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Imputed Sites Count
# MAGIC ```dax
# MAGIC Imputed_Sites = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     'gold_plan_abastecimiento'[imputed_flag] = TRUE()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Sites with Active PO
# MAGIC ```dax
# MAGIC Active_PO_Sites = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     'gold_plan_abastecimiento'[has_purchase_order] = TRUE()
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4. Dynamic Risk Score Calculations
# MAGIC %md
# MAGIC ## 4. Dynamic Risk Score Calculations
# MAGIC
# MAGIC ### Dynamic Risk Score (Composite)
# MAGIC ```dax
# MAGIC Dynamic_Risk_Score = 
# MAGIC VAR StockoutPct = DIVIDE([Stockout_Count], [Total_PUS], 0) * 100
# MAGIC VAR CriticalPct = DIVIDE([Critical_Stock_Count], [Total_PUS], 0) * 100
# MAGIC VAR AvgCoverage = [Avg_Coverage_Days]
# MAGIC VAR HighVolRisk = DIVIDE([High_Volume_At_Risk], [Total_PUS], 0) * 100
# MAGIC RETURN
# MAGIC (
# MAGIC     (StockoutPct * 0.4) +          -- 40% weight on stockouts
# MAGIC     (CriticalPct * 0.3) +           -- 30% weight on critical sites
# MAGIC     (IF(AvgCoverage < 15, 20, 0)) + -- 20 points if avg < 15 days
# MAGIC     (HighVolRisk * 0.1)             -- 10% weight on high-volume risk
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Risk Trend Indicator (Month-over-Month)
# MAGIC ```dax
# MAGIC Risk_Trend_MoM = 
# MAGIC VAR CurrentRisk = [Dynamic_Risk_Score]
# MAGIC VAR PreviousRisk = 
# MAGIC     CALCULATE(
# MAGIC         [Dynamic_Risk_Score],
# MAGIC         DATEADD('gold_plan_abastecimiento'[processing_date], -1, MONTH)
# MAGIC     )
# MAGIC RETURN
# MAGIC DIVIDE(CurrentRisk - PreviousRisk, PreviousRisk, 0) * 100
# MAGIC ```
# MAGIC
# MAGIC ### Risk Category Classification
# MAGIC ```dax
# MAGIC Risk_Category = 
# MAGIC VAR RiskScore = [Dynamic_Risk_Score]
# MAGIC RETURN
# MAGIC SWITCH(
# MAGIC     TRUE(),
# MAGIC     RiskScore >= 70, "🔴 Critical Risk",
# MAGIC     RiskScore >= 50, "🟡 High Risk",
# MAGIC     RiskScore >= 30, "🟡 Moderate Risk",
# MAGIC     RiskScore >= 15, "🟢 Low Risk",
# MAGIC     "🟢 Minimal Risk"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,5. Conditional Formatting Logic
# MAGIC %md
# MAGIC ## 5. Conditional Formatting Logic
# MAGIC
# MAGIC ### KPI Card Background Color (Critical Stockout %)
# MAGIC ```dax
# MAGIC KPI_BG_Color_Critical = 
# MAGIC VAR Pct = [Critical_Stockout_Pct]
# MAGIC RETURN
# MAGIC SWITCH(
# MAGIC     TRUE(),
# MAGIC     Pct >= 30, "#DC3545",  -- Red (Critical)
# MAGIC     Pct >= 15, "#FFC107",  -- Yellow (Warning)
# MAGIC     "#28A745"              -- Green (Good)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Table Row Color (Dispatch Queue)
# MAGIC ```dax
# MAGIC Row_Color_Dispatch = 
# MAGIC SWITCH(
# MAGIC     'gold_plan_abastecimiento'[ESTADO_INVENTARIO],
# MAGIC     "DESABASTECIDO", "#FFEBEE",  -- Soft Red
# MAGIC     "CRÍTICO", "#FFF3E0",          -- Soft Orange
# MAGIC     "ABASTECIDO", "#E8F5E9",     -- Soft Green
# MAGIC     "#FFFFFF"                     -- White (default)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Coverage Bracket Icon
# MAGIC ```dax
# MAGIC Coverage_Icon = 
# MAGIC VAR Days = 'gold_plan_abastecimiento'[SALDO_DIAS]
# MAGIC RETURN
# MAGIC SWITCH(
# MAGIC     TRUE(),
# MAGIC     Days <= 0, "🔴",
# MAGIC     Days <= 5, "⚠️",
# MAGIC     Days <= 15, "🟡",
# MAGIC     Days <= 30, "🟢",
# MAGIC     "✅"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Status Badge Text Color
# MAGIC ```dax
# MAGIC Status_Text_Color = 
# MAGIC SWITCH(
# MAGIC     'gold_plan_abastecimiento'[ESTADO_INVENTARIO],
# MAGIC     "DESABASTECIDO", "#FFFFFF",
# MAGIC     "CRÍTICO", "#000000",
# MAGIC     "ABASTECIDO", "#FFFFFF",
# MAGIC     "#000000"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,6. Time Intelligence Measures
# MAGIC %md
# MAGIC ## 6. Time Intelligence Measures
# MAGIC
# MAGIC ### Yesterday's Stockout Count
# MAGIC ```dax
# MAGIC Stockout_Yesterday = 
# MAGIC CALCULATE(
# MAGIC     [Stockout_Count],
# MAGIC     DATEADD('gold_plan_abastecimiento'[processing_date], -1, DAY)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Week-over-Week Change (Stockouts)
# MAGIC ```dax
# MAGIC Stockout_WoW_Change = 
# MAGIC VAR Current = [Stockout_Count]
# MAGIC VAR LastWeek = 
# MAGIC     CALCULATE(
# MAGIC         [Stockout_Count],
# MAGIC         DATEADD('gold_plan_abastecimiento'[processing_date], -7, DAY)
# MAGIC     )
# MAGIC RETURN
# MAGIC Current - LastWeek
# MAGIC ```
# MAGIC
# MAGIC ### Rolling 7-Day Average (Critical Sites)
# MAGIC ```dax
# MAGIC Critical_7Day_Avg = 
# MAGIC CALCULATE(
# MAGIC     AVERAGE('gold_plan_abastecimiento'[Critical_Stock_Count]),
# MAGIC     DATESINPERIOD(
# MAGIC         'gold_plan_abastecimiento'[processing_date],
# MAGIC         LASTDATE('gold_plan_abastecimiento'[processing_date]),
# MAGIC         -7,
# MAGIC         DAY
# MAGIC     )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Month-to-Date Network Health
# MAGIC ```dax
# MAGIC MTD_Network_Health = 
# MAGIC CALCULATE(
# MAGIC     [Network_Health_Index],
# MAGIC     DATESMTD('gold_plan_abastecimiento'[processing_date])
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,7. Geo-Spatial Measures (for Map Visual)
# MAGIC %md
# MAGIC ## 7. Geo-Spatial Measures (for Map Visual)
# MAGIC
# MAGIC ### Sites per Department
# MAGIC ```dax
# MAGIC Sites_Per_Department = 
# MAGIC CALCULATE(
# MAGIC     COUNTROWS('gold_plan_abastecimiento'),
# MAGIC     ALLEXCEPT('gold_plan_abastecimiento', 'gold_plan_abastecimiento'[DEPARTAMENTO])
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Department Risk Level
# MAGIC ```dax
# MAGIC Dept_Risk_Level = 
# MAGIC VAR DeptStockouts = 
# MAGIC     CALCULATE(
# MAGIC         [Stockout_Count],
# MAGIC         ALLEXCEPT('gold_plan_abastecimiento', 'gold_plan_abastecimiento'[DEPARTAMENTO])
# MAGIC     )
# MAGIC VAR DeptTotal = [Sites_Per_Department]
# MAGIC VAR RiskPct = DIVIDE(DeptStockouts, DeptTotal, 0) * 100
# MAGIC RETURN
# MAGIC SWITCH(
# MAGIC     TRUE(),
# MAGIC     RiskPct >= 40, "🔴 High",
# MAGIC     RiskPct >= 20, "🟡 Medium",
# MAGIC     "🟢 Low"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Map Bubble Size (by total rolls)
# MAGIC ```dax
# MAGIC Map_Bubble_Size = 
# MAGIC SUM('gold_plan_abastecimiento'[SALDO_ROLLOS])
# MAGIC ```
# MAGIC
# MAGIC ### Map Color Intensity (stockout density)
# MAGIC ```dax
# MAGIC Map_Color_Intensity = 
# MAGIC VAR DeptStockouts = [Stockout_Count]
# MAGIC VAR DeptSites = [Sites_Per_Department]
# MAGIC RETURN
# MAGIC DIVIDE(DeptStockouts, DeptSites, 0)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Power BI Visual Configuration Guide
# MAGIC %md
# MAGIC ## Power BI Visual Configuration Guide
# MAGIC
# MAGIC ### Page 1: Logistics Control Tower
# MAGIC
# MAGIC **KPI Card 1: Total PUS**
# MAGIC * Measure: `[Total_PUS]`
# MAGIC * Format: `#,##0`
# MAGIC * Trend: None
# MAGIC
# MAGIC **KPI Card 2: Network Health Index**
# MAGIC * Measure: `[Network_Health_Index]`
# MAGIC * Format: `0.0 %`
# MAGIC * Goal: `85%`
# MAGIC * Trend: `[Risk_Trend_MoM]`
# MAGIC
# MAGIC **KPI Card 3: Critical Stock Locations**
# MAGIC * Measure: `[Critical_Stock_Count]`
# MAGIC * Format: `#,##0`
# MAGIC * Background: `[KPI_BG_Color_Critical]` (conditional)
# MAGIC
# MAGIC **KPI Card 4: Confirmed Stockouts**
# MAGIC * Measure: `[Stockout_Count]`
# MAGIC * Format: `#,##0`
# MAGIC * Trend: `[Stockout_WoW_Change]`
# MAGIC
# MAGIC **KPI Card 5: Total Freight Budget**
# MAGIC * Measure: `[Total_Freight_Budget]`
# MAGIC * Format: `$ #,##0`
# MAGIC
# MAGIC **Map Visual:**
# MAGIC * Location: `DEPARTAMENTO`, `MUNICIPIO`
# MAGIC * Size: `[Map_Bubble_Size]`
# MAGIC * Color: `[Dept_Risk_Level]`
# MAGIC * Tooltip: Add `ESTADO_INVENTARIO`, `SALDO_DIAS`
# MAGIC
# MAGIC **Donut Chart:**
# MAGIC * Legend: `ESTADO_INVENTARIO`
# MAGIC * Values: `[Sites_By_Bracket]`
# MAGIC
# MAGIC ### Page 2: Coverage Analytics
# MAGIC
# MAGIC **Stacked Bar Chart:**
# MAGIC * Axis: `T` (Coverage Bracket)
# MAGIC * Values: `[Sites_By_Bracket]`
# MAGIC * Legend: `TIPOLOGIA OPERACIONES`
# MAGIC
# MAGIC **Scatter Plot:**
# MAGIC * X-Axis: `PROM_TRANSACCIONES`
# MAGIC * Y-Axis: `SALDO_ROLLOS`
# MAGIC * Size: `PPTO_TRANSP`
# MAGIC * Color: `ESTADO_INVENTARIO`
# MAGIC
# MAGIC ### Page 3: Dispatch Queue
# MAGIC
# MAGIC **Table Visual:**
# MAGIC * Columns: `cncodpus`, `NOMBREPUS`, `DEPARTAMENTO`, `SALDO_ROLLOS`, `SALDO_DIAS`, `T`, `ESTADO_INVENTARIO`
# MAGIC * Sort By: `[Dispatch_Priority_Score]` (Descending)
# MAGIC * Conditional Formatting: Background = `[Row_Color_Dispatch]`
# MAGIC
# MAGIC **Slicers:**
# MAGIC * DEPARTAMENTO (Dropdown)
# MAGIC * SEDE ROLLOS (Dropdown)
# MAGIC * ESTADO_INVENTARIO (Checkbox)
# MAGIC * SALDO_DIAS (Range slider: 0-90)

# COMMAND ----------

