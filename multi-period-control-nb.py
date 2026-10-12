# Databricks notebook source
# MAGIC %md ## Control Notebook

# COMMAND ----------

# MAGIC %md Use this notebook to run multiple periods of 01_cmm_pos_feed notebook

# COMMAND ----------

# MAGIC %run /Shared/KrPrtnrs_service_principal

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

dbutils.widgets.text(
  name = 'our_brands', 
  defaultValue = 'False', 
  label = 'our_brands'
)

# Set dates and variables:
fiscal_start = dbutils.widgets.get('fiscal_start')
fiscal_end = dbutils.widgets.get('fiscal_end')
our_brands = dbutils.widgets.get('our_brands')


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

for rows in dates.select("start_date", "end_date").collect():

  dbutils.notebook.run(
    "/Repos/l920466@8451.com/ap-feed-hhs/01_cmm_pos_feed_adweek", 
    timeout_seconds=60000,
    arguments={'start_date':rows[0], 'end_date':rows[1], 'our_brands':our_brands}
  )
