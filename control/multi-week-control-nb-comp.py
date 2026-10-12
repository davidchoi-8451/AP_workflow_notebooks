# Databricks notebook source
# MAGIC %md ## Control Notebook

# COMMAND ----------

# MAGIC %md Use this notebook to run multiple weeks of 01_cmm_pos_feed notebook

# COMMAND ----------

# MAGIC %run /Shared/KrPrtnrs_service_principal

# COMMAND ----------

# Downgrade protobuf package
%pip install protobuf==3.20.1

# COMMAND ----------

# import
import pyspark.sql.functions as f
from kayday import KrogerDate
from effodata import ACDS
import sys
sys.path.append('/Workspace/Repos/j327144@8451.com/j327144_utils')

# COMMAND ----------

# parameters
dbutils.widgets.text(
  name = "fiscal_start", 
  defaultValue = "", 
  label = "Fiscal Start (YYYYPDWK)")

dbutils.widgets.text(
  name = "fiscal_end", 
  defaultValue = "", 
  label = "Fiscal End (YYYYPDWK)")

# Set dates and variables:
fiscal_start = dbutils.widgets.get('fiscal_start')
fiscal_end = dbutils.widgets.get('fiscal_end')

wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'

# COMMAND ----------

# data
acds = ACDS(use_sample_mart=True)

# COMMAND ----------

# define dates df based on parameter range
dates = acds.dates\
  .select('fiscal_month', 'fiscal_week', 'date')\
  .withColumn('fiscal_period', f.concat(f.col('fiscal_month'), f.lit("04")))\
  .filter(f.col('fiscal_week').between(fiscal_start, fiscal_end))\
  .groupBy('fiscal_month')\
  .agg(f.max('date').alias('max_date'))\
    .select(f.date_format('max_date', 'yyyy-MM-dd').alias('max_date')).distinct()

# Convert DataFrame to a list of dates
date_list = dates.select("max_date").rdd.flatMap(lambda x: x).collect()
print(date_list)

# COMMAND ----------

# Loop through dates to execute 01_cmm_pos_feed notebook
# inputs are dates from above.

for date in date_list:

  dbutils.notebook.run(
    "/Repos/l920466@8451.com/ap-feed-hhs/035_competitor_effogeo", 
    timeout_seconds=60000,
    arguments={'date':date}
  )
