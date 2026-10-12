# Databricks notebook source
# MAGIC %md
# MAGIC ## CMM POS Feed: Omnichannel Model

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

# MAGIC %run "./file_utils_nb"

# COMMAND ----------

from pyspark import storagelevel
from pyspark import SparkContext
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import col, when, count, countDistinct
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
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.default.parallelism", "264")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

# Widgets

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

# Set dates and variables:
db = 'l920466_cmm'
#spark.sql(f'create schema if not exists {db}')
run_id = ''.join(random.choice(string.ascii_letters) for x in range(5))
start_date = dbutils.widgets.get('start_date')
end_date = dbutils.widgets.get('end_date')
fiscal_week = KrogerDate(date=end_date).format_week().replace("F", "")
fiscal_week_prev = (KrogerDate(date=end_date) - timedelta(weeks=1)).format_week().replace("F", "")
fiscal_week_prev_month = (KrogerDate(date=end_date) - timedelta(weeks=4)).format_week().replace("F", "")
start_week = KrogerDate(date=start_date).format_week().replace("F", "")
end_week = fiscal_week
mode = 'overwrite'
print_output = False
write_output = True
use_sample_mart = False
modality_name = 'enterprise_nonship'

# print parameters
print("run_id: " + run_id)
print("start date: " + start_date)
print("end_date: " + end_date)
print("start_week: " + start_week)
print("end_week: " + end_week)
print("fiscal_week: " + fiscal_week)
print("fiscal_week_prev: " + fiscal_week_prev)
print("last week of previous month: " + fiscal_week_prev_month)
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
      raise

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
pref_div = spark.read.parquet(
  "abfss://acds@sa8451posprd.dfs.core.windows.net" + 
  "/preferred_store/fiscal_week=" + fiscal_week)\
  .withColumn('pref_div', f.substring(f.col('pref_store_code_1'), 1, 3))

if print_output:
  pref_div.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bigfoot Vintages

# COMMAND ----------

# added 2024-01-09
# read in bigfoot vintages for instore and ecomm, and combine to get latest purchase
# Select min of first week they ever shopped to get first occurence, and get min of weeks since mod visit to get most recent time elapsed since visit

# If customer is in store in 2020 and ecomm in 2023, vintage wk would be 2020
# if customer has not shopped in store since 2020 but delivered last wk, weeks since mod visit would be 1
bf_instore = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=instore/end_week={fiscal_week}')

bf_instore_prev = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=instore/end_week={fiscal_week_prev_month}')\
  .select('ehhn', 'vintage_week', 'weeks_since_mod_visit')

bf_ecomm = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={fiscal_week}')

bf_ecomm_prev = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={fiscal_week_prev_month}')\
  .select('ehhn', 'vintage_week', 'weeks_since_mod_visit')

# last week vintage vs this weeks vintage 
bf_combined = bf_instore.union(bf_ecomm).groupBy('ehhn').agg(f.min(f.col('vintage_week')).alias('vintage_week'), 
                                                             f.min(f.col('weeks_since_mod_visit')).alias('weeks_since_mod_visit'))

bf_combined_prev = bf_instore_prev.union(bf_ecomm_prev).groupBy('ehhn').agg(f.min(f.col('vintage_week')).alias('vintage_week'), 
                                                             f.min(f.col('weeks_since_mod_visit')).alias('weeks_since_mod_visit'))

bf_combined.limit(10).display()
bf_combined_prev.limit(10).display()

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
# MAGIC #### Create Lapsed segment

# COMMAND ----------

# dates = acds.dates.select('fiscal_week', 'fiscal_month').distinct().withColumnRenamed('fiscal_Week', 'vintage_week')

# COMMAND ----------

# fiscal_week = [20240701, 20240702, 20240703, 20240704, 20240801, 20240802, 20240803, 20240804, 20240901, 20240902, 20240903, 20240904, 20241001, 20241002, 20241003, 20241004]

# bf_omni_df = None

# for week in fiscal_week:
#   bf_instore = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=instore/end_week={week}')
#   bf_ecomm = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={week}')

#   bf_combined = bf_instore.union(bf_ecomm).groupBy('ehhn').agg(f.min(f.col('vintage_week')).alias('vintage_week'), 
#                                                              f.min(f.col('weeks_since_mod_visit')).alias('weeks_since_mod_visit'))

#   temp_df = bf_combined\
#     .join(dates, how = 'inner', on = 'vintage_week') \
#     .withColumn('omni_seg', f.when(f.col('vintage_week') == week, 'new')) \
#     .select('ehhn', 'vintage_week', 'fiscal_month','omni_seg') \
#       .filter(f.col('omni_seg') == 'new')
#   if bf_omni_df is None:
#     bf_omni_df = temp_df
#   else:
#     bf_omni_df = bf_omni_df.unionByName(temp_df)

# bf_omni_df.groupBy('fiscal_month').agg(f.countDistinct('ehhn').alias('new_hhs')).display()

# COMMAND ----------

# fiscal_week = [20250701, 20250702, 20250703, 20250704, 20250801, 20250802, 20250803, 20250804, 20250901, 20250902, 20250903, 20250904, 20251001, 20251002, 20251003, 20251004]

# bf_omni_df = None

# for week in fiscal_week:
#   bf_instore = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=instore/end_week={week}')
#   bf_ecomm = spark.read.parquet(abfss.path_digitalinsights + f'/customer_data_assets/bigfoot/prod/modality=ecommerce/end_week={week}')

#   bf_combined = bf_instore.union(bf_ecomm).groupBy('ehhn').agg(f.min(f.col('vintage_week')).alias('vintage_week'), 
#                                                              f.min(f.col('weeks_since_mod_visit')).alias('weeks_since_mod_visit'))

#   temp_df = bf_combined\
#     .join(dates, how = 'inner', on = 'vintage_week') \
#     .withColumn('omni_seg', f.when(f.col('vintage_week') == week, 'new')) \
#     .select('ehhn', 'vintage_week', 'fiscal_month','omni_seg') \
#       .filter(f.col('omni_seg') == 'new')
#   if bf_omni_df is None:
#     bf_omni_df = temp_df
#   else:
#     bf_omni_df = bf_omni_df.unionByName(temp_df)

# bf_omni_df.groupBy('fiscal_month').agg(f.countDistinct('ehhn').alias('new_hhs')).display()

# COMMAND ----------

# Base customer group is all ehhns in bigfoot: ecommerce + instore modality
# logic for lapsed customers is based on 'number of weeks since last visit' 
# as defined in the bigfoot dataset

# Determine segmentation based on prior week's vintage
# This is needed because in the next step, a modifier will be add to 'reactivated'
# households based on their prior lapsed behavior from the prior week.
enterprise_seg_prev = bf_combined_prev\
  .withColumn(
    'enterprise_seg_prev', 
    f.when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks')\
        .when(f.col('weeks_since_mod_visit') >= 52, 'lapsed_52_plus_wks')\
    .otherwise(None))\
  .select('ehhn', 'enterprise_seg_prev')

# EHHN and category (whether EHHN is active or lapsed)
enterprise_seg_prev.display()

# CURRENT WEEK: Determine current segmentation with 'reactivated' and prev segment modifier for reactivated.
# Reactivated means that their weeks since last visit between 0 to 3 to show they've started visiting anytime the last month
enterprise_seg_curr = bf_combined\
  .join(enterprise_seg_prev, on = 'ehhn', how = 'left')\
  .withColumn('enterprise_seg', 
    f.when(((f.col('vintage_week') >= start_week) & (f.col('vintage_week') <= end_week)), 'new')\
    .when(
      # Was previously lapsed, but visited within the last 3 week -> Reactivated
      (f.col('weeks_since_mod_visit').isin(0,1,2,3)) &
      (f.col('enterprise_seg_prev') == 'lapsed_52_plus_wks'), 'active_reactivated_from_lapsed_52_plus_wks')\
    .when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks')\
      .when(f.col('weeks_since_mod_visit')>=52, 'lapsed_52_plus_wks') \
    .otherwise(None))\
  .select('ehhn', 'enterprise_seg', 'weeks_since_mod_visit') \
  .filter(f.col('enterprise_seg').isin('new', 'active_reactivated_from_lapsed_52_plus_wks'))

enterprise_seg_curr.display()

# COMMAND ----------

#QA
enterprise_seg_curr.groupBy('enterprise_seg').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### April 2026 Edit: Add Existing Last 52 Weeks HHs 
# MAGIC - Section for just my changes for visibility

# COMMAND ----------

# Base customer group is all ehhns in bigfoot: ecommerce + instore modality
# logic for lapsed customers is based on 'number of weeks since last visit' 
# as defined in the bigfoot dataset

# Determine segmentation based on prior week's vintage
# This is needed because in the next step, a modifier will be add to 'reactivated'
# households based on their prior lapsed behavior from the prior week.
enterprise_seg_prev = bf_combined_prev\
  .withColumn(
    'enterprise_seg_prev', 
    f.when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks')\
        .when(f.col('weeks_since_mod_visit') >= 52, 'lapsed_52_plus_wks')\
    .otherwise(None))\
  .select('ehhn', 'enterprise_seg_prev')

# EHHN and category (whether EHHN is active or lapsed)
enterprise_seg_prev.display()

# CURRENT WEEK: Determine current segmentation with 'reactivated' and prev segment modifier for reactivated.
# Reactivated means that their weeks since last visit between 0 to 3 to show they've started visiting anytime the last month
enterprise_seg_curr = bf_combined\
  .join(enterprise_seg_prev, on = 'ehhn', how = 'left')\
  .withColumn('enterprise_seg', 
    f.when(((f.col('vintage_week') >= start_week) & (f.col('vintage_week') <= end_week)), 'new')\
    .when(
      # Was previously lapsed, but visited within the last 3 week -> Reactivated
      (f.col('weeks_since_mod_visit').isin(0,1,2,3)) &
      (f.col('enterprise_seg_prev') == 'lapsed_52_plus_wks'), 'active_reactivated_from_lapsed_52_plus_wks')\
    .when(f.col('weeks_since_mod_visit') < 52, 'active_under_52_wks')\
      .when(f.col('weeks_since_mod_visit')>=52, 'lapsed_52_plus_wks') \
    .otherwise(None))\
  .select('ehhn', 'enterprise_seg', 'weeks_since_mod_visit') \
  .filter(f.col('enterprise_seg').isin('new', 'active_under_52_wks', 'active_reactivated_from_lapsed_52_plus_wks'))

enterprise_seg_curr.display()

# Checking to see if existing 52 Wks HHs are also included
enterprise_seg_curr.groupBy('enterprise_seg').count().display()

# COMMAND ----------

# # Are there any new HHs that this methodology is missing - check in Vintages for same time period?
# # Still working on figuring out why these HHs wouldn't be in Vintages
# test = enterprise_seg_curr.filter('enterprise_seg == "new"')
# missing = test.join(vintages_ent, how = 'left_anti', on = 'ehhn')
# missing.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transactions

# COMMAND ----------

# Get transactions for the weeks in question. Added pref_div column for grouping

txrns = acds\
  .get_transactions(
    start_date = start_date,
    end_date = end_date, 
    apply_golden_rules = grs_new, 
    query_filters = ['gtin_no <> "PAPER KROGER"',
                      'gtin_no <> "PAPER MANUF."'],
    join_with=["dates", "stores", "products"],
    join_type="left")\
      .join(pref_div, on = 'ehhn', how = 'left')\
      .join(enterprise_seg_curr, how = 'inner', on = 'ehhn')\
   .withColumn('pref_div',
      f.when((col('pref_div').isNull()) & (col('ehhn').isNull()),'No Loyalty Card')\
      .when((col('pref_div').isNull()) & (col('ehhn').isNotNull()), 'Missing Pref Div')\
      .when((col('pref_div').isNotNull()) & (col('ehhn').isNotNull()), col('pref_div'))\
      .otherwise('error')) \
    .withColumn('enterprise_seg',
      f.when((col('enterprise_seg').isNull()) & (col('ehhn').isNull()),'No Loyalty Card')\
      .when((col('enterprise_seg').isNull()) & (col('ehhn').isNotNull()), 'Missing Enterprise Seg')\
      .when((col('enterprise_seg').isNotNull()) & (col('ehhn').isNotNull()), col('enterprise_seg'))\
      .otherwise('error')) \
  .select(
'ehhn', 'cus_act_no', 'gtin_no', 'sto_no', 'mgt_div_no', 'promo_cycle_week', 'pref_div',
    'enterprise_seg', 'modality', 'transaction_code', 'trn_dt', f.col('pid_fyt_rec_dpt_dsc_tx').alias('dept'), 
    'trn_tm', 'store_code', 'net_spend_amt', 'fiscal_week', 'scn_unt_qy', 'loyalty_disc_amt', 'manufacturer_disc_amt', 
    'match_disc_amt', 'kroger_disc_amt', 'item_snap_flg', 'rtl_dol_am', 'item_wic_flg') \
  .cache()

txrns.limit(50).display()

# for testing, dont write to table. Just print transactions table 

# # write delta table, optimize and cache table

spark.sql(f'DROP TABLE IF EXISTS {db}.transactions_{run_id}')
spark.sql(f'DROP TABLE IF EXISTS transactions_{run_id}_cache')
txrns.write.format('delta').saveAsTable(f'{db}.transactions_{run_id}')
spark.sql(f"OPTIMIZE {db}.transactions_{run_id} ZORDER BY (transaction_code)")
spark.sql(f"CACHE TABLE transactions_{run_id}_cache OPTIONS('storageLevel' 'DISK_ONLY') SELECT * FROM {db}.transactions_{run_id};")
transactions = spark.read.table(f'transactions_{run_id}_cache')

assert transactions.filter(col('pref_div').contains('error')).count() == 0, 'pref_div column contains errors'
assert transactions.filter(col('enterprise_seg').contains('error')).count() == 0, 'enterprise_seg column contains errors'

if print_output:
  transactions.limit(10).toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Delivery

# COMMAND ----------

# get delivery modality transactions with spend > 0

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
# MAGIC #### Pickup

# COMMAND ----------

# pickup transactions less any of the outliers below
# households with 1 or more transactions in different divisions on the same date
# households with 4 or more visits on the same day
# households with transactions in more than 1 store or more than one card in the same date and time

pickup_txn = transactions\
  .filter((col('modality')=='PICKUP'))\
  .select('transaction_code','trn_dt','trn_tm','store_code','mgt_div_no','cus_act_no','ehhn')

pu_outlier_1 = pickup_txn\
  .groupBy('ehhn','trn_dt')\
  .agg(f.countDistinct('mgt_div_no').alias('num_div'))\
  .filter(col('num_div')>1)\
  .select('ehhn','trn_dt')
        
pu_outlier_2 = pickup_txn\
  .groupBy('ehhn','trn_dt')\
  .agg(f.countDistinct('transaction_code').alias('num_visits'))\
  .filter(col('num_visits')>4)\
  .select('ehhn','trn_dt')
        
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
# MAGIC #### Combine

# COMMAND ----------

# Combine pickup and delivery to transactions 
# add enterprise vintages

final_txn = transactions.alias('a')\
  .join(pickup_txn_clean.alias('f'),'transaction_code',how='left')\
  .join(delivery_txn_clean.alias('g'),'transaction_code',how='left')\
  .select('a.transaction_code')\
  .distinct()

if print_output:
  print(f"{final_txn.count()} rows in final_txn")
  print("Sample 10 rows of final_txn: \n")
  final_txn.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reports

# COMMAND ----------

# MAGIC %md
# MAGIC #### Store

# COMMAND ----------

basket_store = transactions.alias('a')\
  .join(final_txn.alias('b'),'transaction_code')\
  .groupBy('a.pref_div', 'a.enterprise_seg','a.fiscal_week')\
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
      )


if print_output:
  print(f"{basket_store.count()} rows in basket_store")
  print("Sample 10 rows of basket_store: \n")
  basket_store.limit(10).display()

if write_output:
  # log_counts(basket_store, path=wd+"/bask_store_ecom_counts", fiscal_week = fiscal_week)
  # store in bask_store_files directory
  create_named_csv(
    basket_store,
    path = wd+"/bask_store_omni_files",
    file_name = "bask_store_omni",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  # add to  /send folder to send to nifi
  create_named_csv(
    basket_store,
    path = wd+"/send",
    file_name = "bask_store_omni",
    fiscal_week = fiscal_week, 
    mode = mode
  
  )

# COMMAND ----------

#basket_store.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Department

# COMMAND ----------

adweek =  spark.read.table("price_promotions_prd.science.effo_adweek_purered_v")

# COMMAND ----------

dept_store = transactions.alias('a')\
  .join(final_txn.alias('b'),'transaction_code')\
  .join(adweek.alias('g'),['promo_cycle_week','store_code','gtin_no'], how = "left")\
  .fillna(0, subset=['adv_typ_cd', 'dpl_typ_cd'])\
  .groupBy('a.pref_div', 'enterprise_seg', 'a.dept', 'a.fiscal_week')\
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
      )

if print_output:
  print(f"{dept_store.count()} rows in dept_store")
  print("Sample 10 rows of dept_store: \n")
  dept_store.limit(10).display()

if write_output:
  # log_counts(dept_store, path=wd+"/dept_store_ecom_counts", fiscal_week=fiscal_week, mode = mode)
  # # store in /bask_store_files
  create_named_csv(
    dept_store,
    path = wd+"/dept_store_omni_files",
    file_name = "dept_store_omni",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  # add to /send directory for send to nifi
  create_named_csv(
    dept_store,
    path = wd+"/send",
    file_name = "dept_store_omni",
    fiscal_week = fiscal_week, 
    mode = mode
  )

# COMMAND ----------

dept_store.display()

# COMMAND ----------

if print_output:
  print(dept_store.select("*").count())
  print(dept_store.select("*").distinct().count())

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {db}.transactions_{run_id}')