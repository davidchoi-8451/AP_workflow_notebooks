# Databricks notebook source
# MAGIC %md ## Control Notebook

# COMMAND ----------

# MAGIC %md Use this notebook to run multiple weeks of 01_cmm_pos_feed notebook

# COMMAND ----------

# MAGIC %run /Shared/KrPrtnrs_service_principal

# COMMAND ----------

# MAGIC %run "./file_utils_nb"

# COMMAND ----------

# import
import pyspark.sql.functions as f
from kayday import KrogerDate
from effodata import ACDS
import sys
sys.path.append('/Workspace/Repos/j327144@8451.com/j327144_utils')
#from file_utils import *

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
  .groupby('fiscal_period', 'fiscal_week')\
  .agg(
    f.min("date").alias('start_date'), 
    f.max('date').alias('end_date'))\
  .orderBy(f.desc('fiscal_period'), f.desc('fiscal_week'))

dates.display()

# COMMAND ----------

# Loop through dates to execute 01_cmm_pos_feed notebook
# inputs are dates from above.
# Replace with 01_cmm_pos_feed_omnichannel_hhs_newlogic for the New HH logic from SotC

for rows in dates.select("start_date", "end_date").collect():
#"/Repos/l920466@8451.com/ap-feed-hhs/01_cmm_pos_feed_omnichannel_hhs", 
  dbutils.notebook.run(
    "/Repos/l920466@8451.com/ap-feed-hhs/Cloned_01_cmm_pos_feed_OMNI_hhs_david_update_april_2026",
    timeout_seconds=60000,
    arguments={'start_date':rows[0], 'end_date':rows[1]}
  )
