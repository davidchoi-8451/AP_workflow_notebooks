# Databricks notebook source
# MAGIC %md ## Control Notebook

# COMMAND ----------

# MAGIC %md Use this notebook to run multiple periods of 01_cmm_pos_feed notebook

# COMMAND ----------

# MAGIC %run /Shared/KrPrtnrs_service_principal

# COMMAND ----------

# Downgrade protobuf package
%pip install protobuf==3.20.1

# COMMAND ----------

import pyspark.sql.functions as f
from kayday import KrogerDate
from effodata import ACDS

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


# COMMAND ----------

# data
acds = ACDS(use_sample_mart=True)

# COMMAND ----------

# define dates df based on parameter range
dates = acds.dates\
  .select('fiscal_month', 'date')\
  .withColumn('fiscal_period', f.concat(f.col('fiscal_month'), f.lit("04")))\
  .filter(f.col('fiscal_period').between(fiscal_start, fiscal_end))\
  .groupby('fiscal_period')\
  .agg(
    f.min("date").alias('start_date'), 
    f.max('date').alias('end_date'))\
  .orderBy(f.desc('fiscal_period'))

dates.display()

# COMMAND ----------

# Loop through dates to execute 01_cmm_pos_feed notebook
# inputs are dates from above.

for row in dates.select("end_date").collect():
  
    dbutils.notebook.run(
        "/Repos/l920466@8451.com/ap-feed-hhs/035_competitor_effogeo",
        timeout_seconds=60000,
        arguments={'date': str(row['end_date'])}
    )
