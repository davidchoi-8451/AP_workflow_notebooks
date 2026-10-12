# Databricks notebook source
# MAGIC %md
# MAGIC ## CMM POS Feed: Ecomm Model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Srv Principal

# COMMAND ----------

# MAGIC %run /Shared/KrPrtnrs_service_principal

# COMMAND ----------

# MAGIC %md
# MAGIC #### Libraries

# COMMAND ----------

# %run "./file_utils_nb"

# COMMAND ----------

from pyspark import storagelevel
from pyspark import SparkContext
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import col, when, count, countDistinct
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import seg
from seg.utils import DateType
from effodata import ACDS, golden_rules, effodata, Joiner, Sifter, Equality
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, Rollup, Cube, available_metrics, get_metrics
from kayday import KrogerDate
import seg
import random
import string

# options
pd.set_option('display.max_columns', 100)
spark.conf.set("spark.databricks.io.cache.enabled", "true")
# Comment these out when I run
#sqlContext.setConf("spark.sql.shuffle.partitions", "auto")
#sqlContext.setConf("spark.default.parallelism", "264")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.default.parallelism", "264")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

# Set dates and variables:

dbutils.widgets.text(
  name = "start_date", 
  defaultValue = "", 
  label = "Cal Start Date (YYYYMMDD)")

dbutils.widgets.text(
  name = "end_date", 
  defaultValue = "", 
  label = "Cal End Date (YYYYMMDD)")

# COMMAND ----------

# define working directory
wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'

# set dates and variables
db = 'j327144_cmm_ecomm'
#spark.sql(f'create schema if not exists {db}')
run_id = ''.join(random.choice(string.ascii_letters) for x in range(5))
start_date = dbutils.widgets.get('start_date')
end_date = dbutils.widgets.get('end_date')
fiscal_week = KrogerDate(date=end_date).format_week().replace("F", "")
fiscal_week_prev = (KrogerDate(date=end_date) - timedelta(weeks=1)).format_week().replace("F", "")
start_week = KrogerDate(date=start_date).format_week().replace("F", "")
end_week = fiscal_week
mode = 'overwrite'
print_output = False
write_output = True
use_sample_mart = False
run_tests = False
save_tests = False

# print parameters
print("run_id: " + run_id)
print("start date: " + start_date)
print("end_date: " + end_date)
print("start_week: " + start_week)
print("end_week: " + end_week)
print("fiscal_week: " + fiscal_week)
print("fiscal_week_prev: " + fiscal_week_prev)
print("write mode: " + mode)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDFs

# COMMAND ----------

# function for creating a named csv file

def create_named_csv(sdf, path, file_name, fiscal_week, mode = 'overwrite'):
  # create a named csv file from dataframe
  sdf_out = sdf\
    .coalesce(1)\
    .write\
    .format('csv')\
    .mode(mode)\
    .option("header", True)\
    .option("quote", "")\
    .save(path+f'/temp_files_{run_id}')

  if mode == 'overwrite':
    # copy temp files to named file
    data_location = path+f'/temp_files_{run_id}'
    files = dbutils.fs.ls(data_location)
    csv_file = [x.path for x in files if x.path.endswith('.csv')][0]
    dbutils.fs.cp(
      csv_file, 
      (path + "/" + file_name + "_" + fiscal_week + "_" + format(datetime.now(), "%Y%m%d_%H%M%S") + '.csv'))
    dbutils.fs.rm(data_location, recurse=True) 
  
def file_exists(path):
  """
  Check to see if file exists in databricks
  """
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise Exception(f"Command could not be parsed or unexpected error: {str(e)}")

# COMMAND ----------

# function for logging dates and counts

def log_counts(sdf, path, fiscal_week, mode = 'overwrite'):
# add fiscal_date to dates_completed.csv for logging counts
  dates_completed = spark.read.csv(path + "/dates_completed.csv", header=True)
  cols = ['fiscal_date', 'record_count']
  vals = [(fiscal_week, sdf.count())]
  new = spark.createDataFrame(vals, cols)
  new_dates_completed = new.union(dates_completed).orderBy(f.col('fiscal_date').desc())
  new_dates_completed.write.mode(mode).csv(path + '/dates_completed', header=True)
  
  # write dates_completed to abfss
  data_location = path + '/dates_completed'
  files = dbutils.fs.ls(data_location)
  csv_file = [x.path for x in files if x.path.endswith('.csv')][0]
  dbutils.fs.cp(csv_file, data_location.rstrip('/') + '.csv')
  dbutils.fs.rm(data_location, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACDS

# COMMAND ----------

# transactions data store
acds = ACDS(use_sample_mart=use_sample_mart)
kpi = KPI(use_sample_mart=use_sample_mart)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preferred Store

# COMMAND ----------

# get weekly
# update to preferred_store_refresh on 2/1/2023
pref_div = spark.read.parquet(
  "abfss://acds@sa8451posprd.dfs.core.windows.net" + 
  "/preferred_store/fiscal_week=" + fiscal_week)\
  .withColumn('pref_div', f.substring(f.col('pref_store_code_1'), 1, 3))

pref_div.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Bigfoot Vintages

# COMMAND ----------

class abfss:
  path_personloyalty = 'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net'
  path_landingzone = 'abfss://landingzone@sa8451entlakegrnprd.dfs.core.windows.net'
  path_sms = 'abfss://sms@sa8451msrmntsciprd.dfs.core.windows.net'
  path_datashuttle = 'abfss://sa8451denblrdatamoverdev.dfs.core.windows.net'
  data = 'abfss://data@sa8451entlakegrnprd.dfs.core.windows.net'
  path_mai = 'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net/MAI'
  path_j327144 = 'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net/Users/j327144'
  path_Measurement = 'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net/Measurement'
  path_digitalinsights = 'abfss://digitalinsights@sa8451diginprd.dfs.core.windows.net'

# COMMAND ----------

# added 2022-10-21
# read in bigfoot vintages and ecom funlo
# new data weekly
# 2025-06-29  new data location
bf_ecom_curr = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={fiscal_week}')\
  .select('ehhn', 'vintage_week', 'weeks_since_mod_visit')

# I need to bring in the previous week's ecomm vintages since the new requirement 
# is to add a modifier to reactivated as in 'reactivated from 13-26 weeks lapsed'
bf_ecom_prev = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={fiscal_week_prev}')\
  .select('ehhn', 'vintage_week', 'weeks_since_mod_visit')

bf_instore = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=instore/end_week={fiscal_week}')\
  .select('ehhn', 'weeks_since_mod_visit')

#if print_output:
bf_ecom_curr.limit(10).display()
bf_ecom_prev.limit(10).display()
bf_instore.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Golden Rules

# COMMAND ----------

# Define golden rules to model what was previously sent to AP
# updated from default to remove wags div 105, add 504 (Ocado)
# remove ehhn_0, cus_act_no_0, and ehhn_null exclusions
# add additional recap dept exclusions: 'CC', 'XX', 'KB', 'DEFAULT_HIERARCHY'

grs = golden_rules()

grs_new = grs\
  .update_exclusion(
    set_name="store_exclusions",
    exclusion_name="division_excl", 
    sql=grs.store_exclusions.division_excl.sql.replace(", '105'", ""))\
  .update_exclusion(
    set_name="store_exclusions",
    exclusion_name="division_excl",
    sql="geo_div_no not in ('011','014','016','018','021','024','025','026','029','034','035','540','531','534','615','620','660','701','703','705','706') and geo_div_no not like '540%'", 
    table="stores")\
  .update_exclusion(
    set_name="product_exclusions",
    exclusion_name="pid_fyt_rec_dpt_cd_excl",
    sql="pid_fyt_rec_dpt_cd in ('06', '30', '34', '58', '78', 'CC', 'XX', 'KB', 'DEFAULT_HIERARCHY')",
    table="products")\
  .remove(set_name="customer_exclusions", exclusion_name=["ehhn_0", "cus_act_no_0", "ehhn_null"])

print("Golden rules used: \n")
grs_new

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modalities

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add ecomm_seg and in_store_seg Segments
# MAGIC Added 2022-10  
# MAGIC [SharePoint](https://8451.sharepoint.com/:x:/r/sites/Projects449/Shared%20Documents/Project%20Repo/CMM%20POS%20Feed/Ecomm%20Update/ecomm_scratch.xlsx?d=w194e86737c3f47e68bad24ec985ffcc6&csf=1&web=1&e=jqGxEp)

# COMMAND ----------

# dates = acds.dates.select('fiscal_week', 'fiscal_month').distinct().withColumnRenamed('fiscal_Week', 'vintage_week')

# COMMAND ----------

# fiscal_week = [20240701, 20240702, 20240703, 20240704, 20240801, 20240802, 20240803, 20240804, 20240901, 20240902, 20240903, 20240904, 20241001, 20241002, 20241003, 20241004]

# bf_ecomm_df = None

# for week in fiscal_week:
#   bf_ecom_curr = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={week}')\
#   .select('ehhn', 'vintage_week', 'weeks_since_mod_visit')

#   temp_df = bf_ecom_curr\
#     .join(dates, how = 'inner', on = 'vintage_week') \
#     .withColumn('ecomm_seg', f.when(f.col('vintage_week') == week, 'new')) \
#     .select('ehhn', 'vintage_week', 'fiscal_month','ecomm_seg') \
#       .filter('ecomm_seg == "new"')
#   if bf_ecomm_df is None:
#     bf_ecomm_df = temp_df
#   else:
#     bf_ecomm_df = bf_ecomm_df.unionByName(temp_df)

# bf_ecomm_df.groupBy('fiscal_month').agg(f.countDistinct('ehhn').alias('new_hhs')).display()

# COMMAND ----------

# fiscal_week = [20250701, 20250702, 20250703, 20250704, 20250801, 20250802, 20250803, 20250804, 20250901, 20250902, 20250903, 20250904, 20251001, 20251002, 20251003, 20251004]

# bf_ecomm_df = None

# for week in fiscal_week:
#   bf_ecom_curr = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={week}')\
#   .select('ehhn', 'vintage_week', 'weeks_since_mod_visit')

#   temp_df = bf_ecom_curr\
#     .join(dates, how = 'inner', on = 'vintage_week') \
#     .withColumn('ecomm_seg', f.when(f.col('vintage_week') == week, 'new')) \
#     .select('ehhn', 'vintage_week', 'fiscal_month','ecomm_seg') \
#       .filter('ecomm_seg == "new"')
#   if bf_ecomm_df is None:
#     bf_ecomm_df = temp_df
#   else:
#     bf_ecomm_df = bf_ecomm_df.unionByName(temp_df)

# bf_ecomm_df.groupBy('fiscal_month').agg(f.countDistinct('ehhn').alias('new_hhs')).display()

# COMMAND ----------

# ecom_seg logic with re-engagement + modifier added on 2022-12-01
# Base customer group is all ehhns in bigfoot: ecommerce modality
# 'new' segment value is based on shopping in an ecomm modality in the reporting week and
# additional logic for lapsed customers is based on 'number of weeks since last ecom visit' 
# as defined in the bigfoot dataset

# Determine segmentation based on prior week's vintage
# This is needed because in the next step, a modifier will be add to 'reactivated'
# households based on their prior lapsed behavior from the prior week.

# Previous week's vintage segmentation is "ecomm_seg_prev"
ecomm_seg_prev = bf_ecom_prev\
  .withColumn(
    'ecomm_seg_prev', 
    f.when(f.col('vintage_week') == fiscal_week_prev, 'new')\
    .when(f.col('weeks_since_mod_visit') < 13, 'active_under_13_wks')\
    .when(
      (f.col('weeks_since_mod_visit') >=13) & 
      (f.col('weeks_since_mod_visit') < 26), 'lapsed_13_25_wks')\
    .when(f.col('weeks_since_mod_visit') >= 26, 'lapsed_26_plus_wks')\
    .otherwise(None))\
  .select('ehhn', 'ecomm_seg_prev')
  
ecomm_seg_prev.limit(10).display()

# Reactivations based on "ecomm seg prev" and "weeks since mod visit"
# Determine current segmentation with 'reactivated' and prev segment modifier for reactivated.
# Reactivated means that their weeks since last visit == 0

# Using previous week's vintage segmentation and comparing it with this week's data (weeks since mod visit)
# to determine re-activations
ecomm_seg_curr = bf_ecom_curr\
  .join(ecomm_seg_prev, on = 'ehhn', how = 'left')\
  .withColumn('ecomm_seg', 
    f.when(f.col('vintage_week') == fiscal_week, 'new')\
    .when(
      (f.col('weeks_since_mod_visit') == 0) & 
      (f.col('ecomm_seg_prev') == 'lapsed_13_25_wks'), 'active_reactivated_from_lapsed_13_25_wks')\
    .when(
      (f.col('weeks_since_mod_visit') == 0) &
      (f.col('ecomm_seg_prev') == 'lapsed_26_plus_wks'), 'active_reactivated_from_lapsed_26_plus_wks')\
    .when(f.col('weeks_since_mod_visit') < 13, 'active_under_13_wks_all_other')\
    .when(
      (f.col('weeks_since_mod_visit') >=13) & 
      (f.col('weeks_since_mod_visit') < 26), 'lapsed_13_25_wks')\
    .when(f.col('weeks_since_mod_visit') >= 26, 'lapsed_26_plus_wks')\
    .otherwise(None))\
  .select('ehhn', 'ecomm_seg', 'weeks_since_mod_visit')
ecomm_seg_curr.limit(10).display()

# COMMAND ----------

#QA
ecomm_seg_prev.groupBy('ecomm_seg_prev').count().display()
ecomm_seg_curr.groupBy('ecomm_seg').count().display()

# COMMAND ----------

# in-store logic 
# base group is all ehhns in bigfoot: instore modality
# additional logic added for active or lapsed based on weeks_since_mod_visit 
in_store_seg = bf_instore\
  .withColumn(
    'in_store_seg', 
    f.when(f.col('weeks_since_mod_visit') >= 52, 'lapsed_52_plus_wks')\
    .when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks')\
  .otherwise(None))\
  .select('ehhn', 'in_store_seg')

in_store_seg.display()
in_store_seg.groupBy('in_store_seg').count().display()

# COMMAND ----------

# not intuitive - 'invalid class sets'.

# For example, sales data from customers who are classified as null in store and lapsed ecomm.
# Adding the logic below to the transaction pull will change the invalid class sets to 'not_segmented'
# in their respective segments. (Making sure sales data from customers who are lapsed and also not visiting stores are excluded)

ecomm_lapsed_values = ['lapsed_26_plus_wks', 'lapsed_13_25_wks']

reclassify_ecomm_seg_logic = f.when(
  ((f.col('in_store_seg') == 'not_segmented') | (f.col('in_store_seg') == 'lapsed_26_plus_wks')) & \
  ((f.col('ecomm_seg') == 'not_segmented') | (f.col('ecomm_seg').isin(*ecomm_lapsed_values))), 
  'not_segmented')\
  .otherwise(f.col('ecomm_seg'))

reclassify_in_store_seg_logic = f.when(
  ((f.col('in_store_seg') == 'not_segmented') | (f.col('in_store_seg') == 'lapsed_26_plus_wks')) & \
  ((f.col('ecomm_seg') == 'not_segmented') | (f.col('ecomm_seg').isin(*ecomm_lapsed_values))), 
  'not_segmented')\
  .otherwise(f.col('in_store_seg'))

# creating combination of attributes and filling not_segmented for missing values in either segment
# then applying logic for reclassifying invalid sets
ecomm_instore_segs = ecomm_seg_curr\
  .join(in_store_seg, on = 'ehhn', how = 'full')\
  .fillna('not_segmented', subset = ['ecomm_seg', 'in_store_seg'])\
  .withColumn('ecomm_seg', reclassify_ecomm_seg_logic)\
  .withColumn('in_store_seg', reclassify_in_store_seg_logic)

ecomm_instore_segs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### April 2026 David Changes
# MAGIC - Add existing Last 52 Weeks HHs "Active_Under_52_Weeks"
# MAGIC - Add Reactivated from Lapsed 52+ Weeks HHs "Active_reactivated_from_lapsed_52_plus_wks"
# MAGIC - Adjust Reclassification logic to account for changes.

# COMMAND ----------

# Previous week's vintage segmentation is "ecomm_seg_prev"
# April 2026: AP only wants existing HHs (weeks since visit < 52 Weeks), HHs reactivated from 52+ weeks of lapsed status (weeks since visit >= 52 weks), and new HHs.
# Change 1: Remove 13-26 and 26+ week windows, replace with 52 week windows similar to Omnichannel
ecomm_seg_prev = bf_ecom_prev.withColumn(
    'ecomm_seg_prev', 
    f.when(f.col('vintage_week') == fiscal_week_prev, 'new') \
     .when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks') \
     .when(f.col('weeks_since_mod_visit') >= 52, 'lapsed_52_plus_wks') \
     .otherwise(None)
).select('ehhn', 'ecomm_seg_prev')
  
ecomm_seg_prev.display()
ecomm_seg_prev.groupBy('ecomm_seg_prev').count().display()

# COMMAND ----------

# April 2026: AP only wants existing HHs (weeks since visit < 52 Weeks), HHs reactivated from 52+ weeks of lapsed status (weeks since visit >= 52 weks), and new HHs.
# Change 1: Added Reactivations for Lapsed 52+ weeks HHs that was prev. missing
# Change 2: Added Existing last 52 week HHs
# Change 3: Filter out lapsed HHs in Ecomm. Only want existing, new, and reactivated HHS.
ecomm_seg_curr = bf_ecom_curr\
  .join(ecomm_seg_prev, on = 'ehhn', how = 'left')\
  .withColumn('ecomm_seg', 
    # New HHs
    f.when(f.col('vintage_week') == fiscal_week, 'new')\

    # 04/06/2026 Change 1: Added reactivations for 52+ weeks lapsed HHs ----
    .when(
      (f.col('weeks_since_mod_visit') == 0) & 
      (f.col('ecomm_seg_prev') == 'lapsed_52_plus_wks'), 'active_reactivated_from_lapsed_52_plus_wks')\
    # Change 1 Stops here ---------

    # 04/06/2026 Change 2: Added existing 52 week HHs here  ----
    .when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks') \
    # Change 2 Stops here ---------

    .otherwise("lapsed_52_plus_wks") 
).select('ehhn', 'ecomm_seg', 'weeks_since_mod_visit') \
.filter(f.col('ecomm_seg').isin('new', 'active_under_52_wks', 'active_reactivated_from_lapsed_52_plus_wks'))
# 04/06/2026 Change 3: Filter out lapsed HHs in Ecomm. Only want existing, new, and reactivated HHS.

ecomm_seg_curr.limit(10).display()
ecomm_seg_curr.groupBy('ecomm_seg').count().display()

# COMMAND ----------

# Scope of in-store segmentations is already 52 weeks.
in_store_seg = bf_instore\
  .withColumn(
    'in_store_seg', 
    f.when(f.col('weeks_since_mod_visit') >= 52, 'lapsed_52_plus_wks')\
    .when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks')\
  .otherwise(None))\
  .select('ehhn', 'in_store_seg')

in_store_seg.display()
in_store_seg.groupBy('in_store_seg').count().display()

# COMMAND ----------

# Reclassification logic: The mix/match of transaction and bigfoot causes some outliers classified in groups that are not intuitive - 'invalid class sets'.
# Example: NULL instore and Lapsed Ecomm becomes -> Not Segmented

# April 2026: Simplified reclassification logic with only 52 week lapsed window.

reclassify_ecomm_seg_logic = f.when(
  ((f.col('in_store_seg') == 'not_segmented') | (f.col('in_store_seg') == 'lapsed_52_plus_wks')) & \
  ((f.col('ecomm_seg') == 'not_segmented') | (f.col('ecomm_seg').isin('lapsed_52_plus_wks'))), 'not_segmented') \
  .otherwise(f.col('ecomm_seg'))

reclassify_in_store_seg_logic = f.when(
  ((f.col('in_store_seg') == 'not_segmented') | (f.col('in_store_seg') == 'lapsed_52_plus_wks')) & \
  ((f.col('ecomm_seg') == 'not_segmented') | (f.col('ecomm_seg').isin('lapsed_52_plus_wks'))), 'not_segmented') \
  .otherwise(f.col('in_store_seg'))

ecomm_instore_segs = ecomm_seg_curr \
  .join(in_store_seg, on = 'ehhn', how = 'full')\
  .fillna('not_segmented', subset = ['ecomm_seg', 'in_store_seg'])\
  .withColumn('ecomm_seg', reclassify_ecomm_seg_logic)\
  .withColumn('in_store_seg', reclassify_in_store_seg_logic)

# COMMAND ----------

ecomm_instore_segs.groupBy('ecomm_seg').count().display()

# COMMAND ----------

#ecomm_seg_curr.filter('vintage_week in (20250701, 20250702, 20250703, 20250704)').filter('').groupBy('vintage_week').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transactions

# COMMAND ----------

# Get transactions for the month in question. 
# 2022-10-24: Added ecomm_seg and in_store_seg fields
# 2022-12-05: Added pref_div column for grouping

# Txrns table contains all transactions within the date range
# Ehhn data + all products bought + preferred division + modality
# + in_store and current seg (calculated from vintages above)
transactions = acds\
  .get_transactions(
    start_date = start_date,
    end_date = end_date, 
    apply_golden_rules = grs_new, 
    query_filters = ['gtin_no <> "PAPER KROGER"',
                  'gtin_no <> "PAPER MANUF."'],
    join_with=["dates", "products"], 
    join_type="left")\
  .join(pref_div, on = 'ehhn', how = 'left')\
  .join(ecomm_instore_segs, on = 'ehhn', how = 'left')\
  .withColumn('pref_div',
      f.when((col('pref_div').isNull()) & (col('ehhn').isNull()),'No Loyalty Card')\
      .when((col('pref_div').isNull()) & (col('ehhn').isNotNull()), 'Missing Pref Div')\
      .when((col('pref_div').isNotNull()) & (col('ehhn').isNotNull()), col('pref_div'))\
      .otherwise('error'))\
  .withColumn('ecomm_seg',
      f.when((col('ecomm_seg').isNull()) & (col('ehhn').isNull()),'No Loyalty Card')\
      .when((col('ecomm_seg').isNull()) & (col('ehhn').isNotNull()), 'Missing Ecomm Seg')\
      .when((col('ecomm_seg').isNotNull()) & (col('ehhn').isNotNull()), col('ecomm_seg'))\
      .otherwise('error'))\
  .withColumn('in_store_seg',
      f.when((col('in_store_seg').isNull()) & (col('ehhn').isNull()), 'No Loyalty Card')\
      .when((col('in_store_seg').isNull()) & col('ehhn').isNotNull(), 'Missing In-Store Seg')\
      .when((col('in_store_seg').isNotNull()) & col('ehhn').isNotNull(), col('in_store_seg'))\
      .otherwise('error'))\
  .select(
    'ehhn', 'cus_act_no', 'gtin_no', 'sto_no', 'mgt_div_no', 'promo_cycle_week', 'pref_div', 'ecomm_seg', 'in_store_seg', 'modality', 'transaction_code', 'trn_dt', 
    'trn_tm', 'store_code', f.col('pid_fyt_rec_dpt_dsc_tx').alias('dept'), 
    'net_spend_amt', 'fiscal_week', 'scn_unt_qy', 'loyalty_disc_amt', 'manufacturer_disc_amt', 
    'match_disc_amt', 'kroger_disc_amt', 'item_snap_flg', 'rtl_dol_am', 'item_wic_flg')\
  .cache()

transactions.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pickup

# COMMAND ----------

# pickup transactions less any of the outliers below
# households with 1 or more transactions in different divisions on the same date
# households with 4 or more visits on the same day
# households with transactions in more than 1 store or more than one card in the same date and time

#txrns 
pickup_txn = transactions\
  .filter((col('modality')=='PICKUP'))\
  .select('transaction_code','trn_dt','trn_tm','store_code','mgt_div_no','cus_act_no','ehhn')

# Pickup order from 2 different geographic divisions on the same day
pu_outlier_1 = pickup_txn\
  .groupBy('ehhn','trn_dt')\
  .agg(f.countDistinct('mgt_div_no').alias('num_div'))\
  .filter(col('num_div')>1)\
  .select('ehhn','trn_dt')

# HH made 4 or more visits on the same day (usually an error)
pu_outlier_2 = pickup_txn\
  .groupBy('ehhn','trn_dt')\
  .agg(f.countDistinct('transaction_code').alias('num_visits'))\
  .filter(col('num_visits')>4)\
  .select('ehhn','trn_dt')

# Hh with transactions in multiple stores at the same date / time
pu_outlier_3 = pickup_txn\
  .groupBy('ehhn','trn_dt','trn_tm')\
  .agg(f.countDistinct('store_code').alias('count_stores'),
  f.countDistinct('cus_act_no').alias('count_cards'))\
  .filter((col('count_stores')>1)|(col('count_cards')>1))\
  .select('ehhn','trn_dt')
        
pu_outliers = pu_outlier_1.union(pu_outlier_2).union(pu_outlier_3)

pickup_txn_clean = pickup_txn.alias('a')\
  .join(pu_outliers.alias('b'),['ehhn','trn_dt'],how='left_anti')\
  .select('a.transaction_code')\
  .withColumn('indicator',f.lit(1))

pickup_txn_clean.display()

if print_output:
  print("Counts: \n")
  print(f"{pickup_txn.count()} rows in pickup_txn")
  print(f"{pu_outlier_1.count()} rows in pu_outlier_1")
  print(f"{pu_outlier_2.count()} rows in pu_outlier_2")
  print(f"{pu_outlier_3.count()} rows in pu_outlier_3")

  print("Sample 10 rows of pu_outliers: \n")
  print(pu_outliers.limit(10).display())

  print("Sample 10 rows of pickup_txn_clean: \n")
  pickup_txn_clean.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Delivery

# COMMAND ----------

# get delivery modality transactions with spend > 0

# replaced transactions -> txrns
delivery_txn = transactions\
  .filter((col('modality')=='DELIVERY'))

# Get all transactions that, when rolled up by transaction code, have spend > 0
delivery_txn_rollup_clean = delivery_txn\
  .groupBy('transaction_code')\
  .agg(f.sum('net_spend_amt').alias('spend'))\
  .filter(col('spend')>0)\
  .select('transaction_code').distinct()

delivery_txn_clean = delivery_txn\
  .join(delivery_txn_rollup_clean.alias('b'),'transaction_code')\
  .select('transaction_code')\
  .withColumn('indicator',f.lit(1))

if print_output:
  print("Counts:")
  print(f"{delivery_txn.count()} rows in delivery_txn")
  print(f"{delivery_txn_rollup_clean.count()} rows in delivery_tnx_rollup_clean")
  print(f"{delivery_txn_clean.count()} rows in delivery_tnx_clean")

  print("Sample 10 rows of delivery_txn_clean: \n")
  delivery_txn_clean.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combine

# COMMAND ----------

# Combine pickup and delivery to transactions and create 'tx_type' column for 
# identifying Ocado, Retail, Ecomm, PU Outlier, Del Outlier or other
# add ecomm and in-store vintages

# txrns instead of transactions
final_txn = transactions.alias('a')\
  .join(pickup_txn_clean.alias('f'),'transaction_code',how='left')\
  .join(delivery_txn_clean.alias('g'),'transaction_code',how='left')\
  .withColumn('tx_type',f.when(col('a.mgt_div_no')=='540','Ocado')\
      .when(col('a.modality')=='INSTORE','Retail')\
      .when((col('a.modality')=='PICKUP')&(col('f.indicator')==1),'Ecomm')\
      .when((col('a.modality')=='PICKUP')&(col('f.indicator').isNull()==True),'PU Outlier')\
      .when((col('a.modality')=='DELIVERY')&(col('g.indicator')==1\
      ),'Ecomm')\
      .when((col('a.modality')=='DELIVERY')&(col('g.indicator').isNull()==True),'Del Outlier')\
      .otherwise('Other'))\
  .select('a.transaction_code','tx_type')\
  .distinct()

if print_output:
  print(f"{final_txn.count()} rows in final_txn")
  print("Sample 10 rows of final_txn: \n")
  final_txn.limit(10).display()

# COMMAND ----------

transactions.filter("modality in ('PICKUP','DELIVERY')") \
  .filter('ecomm_seg == "new"') \
  .groupBy('fiscal_week').agg(f.countDistinct(f.when(col('scn_unt_qy')>0,col('ehhn'))).alias('hhs')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reports

# COMMAND ----------

# MAGIC %md
# MAGIC #### Store

# COMMAND ----------

# replaced transactions w/ txrns
basket_store = transactions.alias('a')\
  .join(final_txn.alias('b'),'transaction_code')\
  .groupBy('a.pref_div', 'ecomm_seg', 'in_store_seg', 'a.fiscal_week', 'b.tx_type')\
  .agg(f.countDistinct(f.when(col('scn_unt_qy')>0,col('a.transaction_code'))).alias('visits')
       ,f.countDistinct(f.when(col('scn_unt_qy')>0,col('a.ehhn'))).alias('hhs')
       ,f.sum('rtl_dol_am').alias('gross_sales')
       ,f.sum('net_spend_amt').alias('net_sales')
       ,f.sum('scn_unt_qy').alias('units')
       ,f.sum('loyalty_disc_amt').alias('loy_card_discount')
       ,f.sum('manufacturer_disc_amt').alias('cpg_coup_giveaway')
       ,f.sum('match_disc_amt').alias('krog_match_giveaway')
       ,f.sum('kroger_disc_amt').alias('krog_coup_giveaway')
       ,f.sum(f.when(col('item_snap_flg')=='Y',col('rtl_dol_am'))).alias('snap_gross_sales')
       ,f.sum(f.when(col('item_snap_flg')=='Y',col('net_spend_amt'))).alias('snap_net_sales')
       ,f.countDistinct(
         f.when(((col('scn_unt_qy')>0)&(col('item_snap_flg')=='Y')),
                col('a.transaction_code'))).alias('snap_visits')
       ,f.sum(f.when(col('item_wic_flg')=='Y',col('rtl_dol_am'))).alias('wic_gross_sales')
       ,f.sum(f.when(col('item_wic_flg')=='Y',col('net_spend_amt'))).alias('wic_net_sales')
       ,f.countDistinct(
         f.when(((col('scn_unt_qy')>0)&(col('item_wic_flg')=='Y')),
                col('a.transaction_code'))).alias('wic_visits')
      )\
  .filter(col('tx_type').isin('Retail','Ecomm','Ocado'))

basket_store.limit(50).display()

if print_output:
  print(f"{basket_store.count()} rows in basket_store")
  print("Sample 10 rows of basket_store: \n")
  basket_store.limit(10).display()

if write_output:
  #log_counts(basket_store, path=wd+"/bask_store_ecom_counts", fiscal_week = fiscal_week)
  # store in bask_store_files directory
  create_named_csv(
    basket_store,
    path = wd+"/bask_store_files",
    file_name = "bask_store_ecom",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  # add to  /send folder to send to nifi
  create_named_csv(
    basket_store,
    path = wd+"/send",
    file_name = "bask_store_ecom",
    fiscal_week = fiscal_week, 
    mode = mode
  )

# COMMAND ----------

basket_store.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Department

# COMMAND ----------

# Changed below to left join on entire adweek data table. Prior logic that narrowed on start and end dates was cutting the first 3 days of the week.
# 20220211: Added fillna() on and and display flags and removed testing for isNull in each case statement.

# COMMAND ----------

adweek =  spark.read.table("price_promotions_prd.science.effo_adweek_purered_v")

# COMMAND ----------

# replaced transactions w/ txrns
dept_store = transactions.alias('a')\
  .join(final_txn.alias('b'),'transaction_code')\
  .join(adweek.alias('g'),['promo_cycle_week','store_code','gtin_no'], how = "left")\
  .fillna(0, subset=['adv_typ_cd', 'dpl_typ_cd'])\
  .groupBy('a.pref_div', 'ecomm_seg', 'in_store_seg', 'a.dept', 'a.fiscal_week', 'b.tx_type')\
  .agg(f.countDistinct(f.when(col('a.scn_unt_qy')>0,col('a.transaction_code'))).alias('visits')
       ,f.countDistinct(f.when(col('a.scn_unt_qy')>0,col('a.ehhn'))).alias('hhs')
       ,f.sum('rtl_dol_am').alias('gross_sales')
       ,f.sum('net_spend_amt').alias('net_sales')
       ,f.sum('a.scn_unt_qy').alias('units')
       ,f.sum('loyalty_disc_amt').alias('loy_card_discount')
       ,f.sum('manufacturer_disc_amt').alias('cpg_coup_giveaway')
       ,f.sum('match_disc_amt').alias('krog_match_giveaway')
       ,f.sum('kroger_disc_amt').alias('krog_coup_giveaway')
       ,f.sum(f.when(((col('adv_typ_cd')!='0')&((col('dpl_typ_cd')=='0'))),
                     col('rtl_dol_am'))).alias('adonly_gross_sales')
       ,f.sum(f.when(((col('adv_typ_cd')!='0')&((col('dpl_typ_cd')=='0'))),
                     col('net_spend_amt'))).alias('adonly_net_sales')
       ,f.sum(f.when(((col('adv_typ_cd')!='0')&((col('dpl_typ_cd')=='0'))),
                     col('loyalty_disc_amt'))).alias('adonly_giveaway')
       ,f.sum(f.when(((col('dpl_typ_cd')!='0')&((col('adv_typ_cd')=='0'))),
                     col('rtl_dol_am'))).alias('displayonly_gross_sales')
       ,f.sum(f.when(((col('dpl_typ_cd')!='0')&((col('adv_typ_cd')=='0'))),
                     col('net_spend_amt'))).alias('displayonly_net_sales')
       ,f.sum(f.when(((col('dpl_typ_cd')!='0')&((col('adv_typ_cd')=='0'))),
                     col('loyalty_disc_amt'))).alias('displayonly_giveaway')
       ,f.sum(f.when(((col('dpl_typ_cd')!='0')&(col('adv_typ_cd')!='0')),
                     col('rtl_dol_am'))).alias('adwdisp_gross_sales')
       ,f.sum(f.when(((col('dpl_typ_cd')!='0')&(col('adv_typ_cd')!='0')),
                     col('net_spend_amt'))).alias('adwdisp_net_sales')
       ,f.sum(f.when(((col('dpl_typ_cd')!='0')&(col('adv_typ_cd')!='0')),
                     col('loyalty_disc_amt'))).alias('adwdisp_giveaway')
       ,f.sum(f.when((((col('adv_typ_cd')=='0'))&(col('dpl_typ_cd')=='0')&(col('tpr_fl')=='Y')),
                     col('rtl_dol_am'))).alias('discountonly_gross_sales')
       ,f.sum(f.when((((col('adv_typ_cd')=='0'))&(col('dpl_typ_cd')=='0')&(col('tpr_fl')=='Y')),
                     col('net_spend_amt'))).alias('discountonly_net_sales')
       ,f.sum(f.when((((col('adv_typ_cd')=='0'))&(col('dpl_typ_cd')=='0')&(col('tpr_fl')=='Y')),
                     col('loyalty_disc_amt'))).alias('discountonly_giveaway')
       ,f.sum(f.when((((col('adv_typ_cd')=='0'))&(col('dpl_typ_cd')=='0')&(col('tpr_fl')=='N')),
                     col('rtl_dol_am'))).alias('notrade_gross_sales')
       ,f.sum(f.when((((col('adv_typ_cd')=='0'))&(col('dpl_typ_cd')=='0')&(col('tpr_fl')=='N')),
                     col('net_spend_amt'))).alias('notrade_net_sales')
       ,f.sum(f.when((((col('adv_typ_cd')=='0'))&(col('dpl_typ_cd')=='0')&(col('tpr_fl')=='N')),
                     col('loyalty_disc_amt'))).alias('notrade_giveaway')
      )\
  .filter(col('tx_type').isin('Retail','Ecomm','Ocado'))

if print_output:
  print(f"{dept_store.count()} rows in dept_store")
  print("Sample 10 rows of dept_store: \n")
  dept_store.limit(10).display()

if write_output:
  #log_counts(dept_store, path=wd+"/dept_store_ecom_counts", fiscal_week=fiscal_week, mode = mode)
  # store in /bask_store_files
  create_named_csv(
    dept_store,
    path = wd+"/dept_store_files",
    file_name = "dept_store_ecom",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  # add to /send directory for send to nifi
  create_named_csv(
    dept_store,
    path = wd+"/send",
    file_name = "dept_store_ecom",
    fiscal_week = fiscal_week, 
    mode = mode
  )

# COMMAND ----------

if print_output:
  print(dept_store.select("*").count())
  print(dept_store.select("*").distinct().count())

# COMMAND ----------

dept_store.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tests

# COMMAND ----------

if run_tests:
  
  # Tests have their own spark session and require access to data outside of this notebook session.
  # therefore, data is written to delta tables below which can be referenced by the new spark session.
  bf_ecom_curr.write.mode('overwrite').format('delta').saveAsTable(f'{db}.bf_ecom_{run_id}')
  bf_instore.write.mode('overwrite').format('delta').saveAsTable(f'{db}.bf_instore_{run_id}')
  ecomm_seg_curr.write.mode('overwrite').format('delta').saveAsTable(f'{db}.ecomm_seg_{run_id}')
  in_store_seg.write.mode('overwrite').format('delta').saveAsTable(f'{db}.in_store_seg_{run_id}')
  basket_store.write.mode('overwrite').format('delta').saveAsTable(f'{db}.basket_store_{run_id}')
  dept_store.write.mode('overwrite').format('delta').saveAsTable(f'{db}.dept_store_{run_id}')
    
  # Programatic tests are located in ./tests directory of this repository
  dbutils.notebook.run('tests/run_unit_tests', timeout_seconds=3000)
  
  if print_output:
    # visual checks
    print('min/max weeks by ecomm_seg value:')
    ecomm_seg_curr\
      .groupBy('ecomm_seg')\
      .agg(
        f.count('ehhn').alias('rows'),
        f.countDistinct('ehhn').alias('unique_hhs'),
        f.min('weeks_since_mod_visit').alias('min_weeks'), 
        f.max('weeks_since_mod_visit').alias('max_weeks'))\
      .orderBy('ecomm_seg')\
      .show()

    print('counts by in_store_seg value:')
    in_store_seg\
      .groupBy('in_store_seg')\
      .agg(
        f.count('ehhn').alias('rows'), 
        f.countDistinct('ehhn').alias('hhs'))\
      .show()

    print('ehhn count by new ecomm_seg and in_store_seg in transactions df:')
    transactions\
      .groupBy('in_store_seg', 'ecomm_seg')\
      .agg(
        f.countDistinct('ehhn').alias('hhs'), 
        f.sum('net_spend_amt'), 
        f.sum(f.when(f.col('ehhn').isNotNull(), f.col('net_spend_amt')).otherwise(None)).alias('carded_spend'),
        f.sum(f.when(f.col('ehhn').isNull(), f.col('net_spend_amt')).otherwise(None)).alias('uncarded_spend'))\
      .show()
  
  # remove test tables
  for table in [f'{db}.bf_ecom_{run_id}', f'{db}.bf_instore_{run_id}', f'{db}.ecomm_seg_{run_id}', 
                f'{db}.in_store_seg_{run_id}', f'{db}.basket_store_{run_id}', f'{db}.dept_store_{run_id}']:
    spark.sql(f'DROP TABLE IF EXISTS table')

# COMMAND ----------

if save_tests:
  save_csv(basket_store, append_timestamp=True, file_name='basket_store', directory = wd + '/test/output')

# COMMAND ----------

# remove transactions table
spark.sql(f'DROP TABLE IF EXISTS {db}.transactions_{run_id}')