# Databricks notebook source
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
import datetime as dt
import seg
from seg.utils import DateType
from effodata import ACDS, golden_rules, effodata, Joiner, Sifter, Equality
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, Rollup, Cube, available_metrics, get_metrics
import kayday as kd
import seg
import random
import string

# COMMAND ----------

# options
pd.set_option('display.max_columns', 100)
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.default.parallelism", "264")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

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
  .select('fiscal_month', 'fiscal_quarter', 'date')\
  .withColumn('fiscal_period', f.concat(f.col('fiscal_month'), f.lit("04")))\
  .filter(f.col('fiscal_period').between(fiscal_start, fiscal_end))\
  .groupby()\
  .agg(
    f.min("date").alias('start_date'), 
    f.max('date').alias('end_date'))

dates.display()

start_date = dates.collect()[0]['start_date']
end_date = dates.collect()[0]['end_date']

# COMMAND ----------

# # define working directory
wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'

# # Set dates and variables
db = 'j327144_cmm'
#spark.sql(f'create schema if not exists {db}')
run_id = ''.join(random.choice(string.ascii_letters) for x in range(5))
fiscal_week = dbutils.widgets.get('fiscal_end')
start_week = dbutils.widgets.get('fiscal_start')
end_week = fiscal_week
mode = 'overwrite'
print_output = True
write_output = True
use_sample_mart = False
# 04-08-2026 Changed modality to "delivery_with_ocado"
modality_name = 'fc_birmingham_vs'


# print parameters
# print("run_id: " + run_id)
print("start date: ", start_date)
print("end_date: ", end_date)
print("start_week: " + start_week)
print("end_week: " + end_week)
print("fiscal_week: " + fiscal_week)
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
      (path + "/" + file_name + "_" + start_week + "_" + fiscal_week + '.csv'))
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
# MAGIC #### Vintages

# COMMAND ----------

# 04/09/2026: using fc_birmingham_vs modality. Keeps only Birmingham fulfillment centers, since all other Ocado FCs are closed.
# No Markets with stores in this filepath, which is OK.
vintages_ocado = spark.read.parquet(f'abfss://digitalinsights@sa8451diginprd.dfs.core.windows.net/customer_data_assets/vintages/hh_{modality_name}')\
  .filter(f'vintage_week between {start_week} and {end_week}')

# COMMAND ----------

#QA number of weeks
vintages_ocado.display()
vintages_ocado.select('vintage_week').distinct().display()

# COMMAND ----------

# added 2023-05-19
# There are about 100k HHs which don't have the specific FC (Fulfillment Center) identified, just that they shopped Ocado. Splitting out FC identified rows and non FC identified rows so i can precisely find transaction for FC identified ones
# FC IDENTIFIED HHs


# 04/09/2026: "Ocado_delivery_all" modality only preserves Birmingham FC. All other FCs are closed.
vintages_ocado_fc = vintages_ocado \
      .filter('vintage_div like "%5400%"') \
      .withColumn('store_code', f.concat(f.col('vintage_div').substr(1, 3),
                                   f.lit('_'),
                                   f.col('vintage_div').substr(4, 5))) \
      .withColumnRenamed('vintage_week', 'fiscal_week') \
        .select('ehhn', 'fiscal_week', 'store_code')


if print_output == True:
  vintages_ocado_fc.display()
  print(vintages_ocado_fc.count())

# COMMAND ----------

# added 2023-05-19
# NON FC IDENTIFIED HHs or EXISTING MARKET HHs

# 04/09/2026: Removing existing market HHs and Non-FC Identified HHs. We are only concerned with
# storeless FC identified HHs. Non-FC and existing market HHs can be found by filtering on other data feeds.

'''
vintages_ocado_div = vintages_ocado\
    .select('ehhn', 'vintage_week', 'vintage_div') \
      .filter('vintage_div == 540 or vintage_div not like "%540%"') \
      .withColumnRenamed('vintage_week', 'fiscal_week') \
        .withColumnRenamed('vintage_div', 'geo_div_no') \
                 .select('ehhn', 'fiscal_week', 'geo_div_no')


if print_output == True:
  vintages_ocado_div.limit(10).display()
  print(vintages_ocado_div.count())
'''

# COMMAND ----------

# QA Check - Can a hh be double counted if they were new to multiple FCs?

# 04/09/2026: Only 1 FC, no double counting happening. Can remove this check.
#vintages_ocado.groupBy('ehhn').count().filter('count > 1').display()

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
# MAGIC #### Transactions

# COMMAND ----------

# Transactions for FC identified HHs
#  Window to find the first Ocado transaction in case there are more than one in the vintage week
window = Window.partitionBy("ehhn").orderBy(col("trn_dt"))

# Get transactions for the weeks in question. 
txrns_fc = acds\
  .get_transactions(
    start_date = start_date,
    end_date = end_date, 
    apply_golden_rules = grs_new, 
    # Store Codes -> All closed storeless Fulfillment Centers
    query_filters = ['store_code not in ("540_00052", "540_00025", "540_00054", "540_00023", "540_00021", "540_00022", "540_00024", "540_00053")',
                     'gtin_no <> "PAPER KROGER"',
                      'gtin_no <> "PAPER MANUF."',
                      "geo_div_no like '540%'"],
    join_with=["dates", "stores"],
    join_type="inner")\
      .join(vintages_ocado_fc, how = 'inner', on = ['ehhn', 'fiscal_week', 'store_code'])\
  .select(
    'ehhn', 'cus_act_no', 'gtin_no', 'sto_no', 'mgt_div_no', 'geo_div_no','modality', 'transaction_code', 'trn_dt', 'date',
    'trn_tm', 'store_code', 'geo_sto_dsc_tx', 'fiscal_week', 'net_spend_amt', 'scn_unt_qy', 'loyalty_disc_amt', 'manufacturer_disc_amt', 
    'match_disc_amt', 'kroger_disc_amt', 'item_snap_flg', 'rtl_dol_am', 'item_wic_flg')\
      .withColumn("rank",f.rank().over(window)) \
        .filter("rank == 1")

txrns_fc.display()

# # write delta table, optimize and cache table

'''
spark.sql(f'DROP TABLE IF EXISTS default.txrns')
spark.sql(f'DROP TABLE IF EXISTS txrns_cache')
txrns_fc.write.format('delta').saveAsTable('default.txrns')
spark.sql(f"OPTIMIZE default.txrns ZORDER BY (transaction_code)")
spark.sql(f"CACHE TABLE txrns_cache OPTIONS('storageLevel' 'DISK_ONLY') SELECT * FROM default.txrns;")
transactions_fc = spark.read.table(f'txrns_cache')

transactions_fc.select('ehhn').dropDuplicates().count()
transactions_fc.limit(10).toPandas()
'''

# COMMAND ----------

#transactions_fc.select('store_code').distinct().display()

# COMMAND ----------

# Transactions for non FC identified HHs
# Window to find the first Ocado transaction in case there are more than one in the vintage week
# Edit on 2.27.2025 - change storeless filter to include Markets with Stores as well

# 04/09/2026: Commented out this code. We do not preserve markets with stores. this can be easily found in different data feeds. Only care about FC identified HHs.
'''
window = Window.partitionBy("ehhn").orderBy(col("trn_dt"))

# Get transactions for the weeks in question. 
txrns_div = acds\
  .get_transactions(
    start_date = start_date,
    end_date = end_date, 
    apply_golden_rules = grs_new, 
    query_filters = ['store_code not in ("540_00052", "540_00025", "540_00054")',
                     'gtin_no <> "PAPER KROGER"',
                      'gtin_no <> "PAPER MANUF."',
                      "mgt_div_no like '540%'"],
    join_with=["dates", "stores"],
    join_type="inner")\
      .join(vintages_ocado_div, how = 'inner', on = ['ehhn', 'fiscal_week', 'geo_div_no'])\
  .select(
    'ehhn', 'cus_act_no', 'gtin_no', 'sto_no', 'mgt_div_no', 'geo_div_no','modality', 'transaction_code', 'trn_dt', 'date',
    'trn_tm', 'store_code', 'geo_sto_dsc_tx', 'fiscal_week', 'net_spend_amt', 'scn_unt_qy', 'loyalty_disc_amt', 'manufacturer_disc_amt', 
    'match_disc_amt', 'kroger_disc_amt', 'item_snap_flg', 'rtl_dol_am', 'item_wic_flg')\
      .withColumn("rank",f.rank().over(window)) \
        .filter("rank == 1")

txrns_div.display()

# # write delta table, optimize and cache table
spark.sql(f'DROP TABLE IF EXISTS default.txrns_div')
spark.sql(f'DROP TABLE IF EXISTS txrns_cache_div')
txrns_div.write.format('delta').saveAsTable('default.txrns_div')
spark.sql(f"OPTIMIZE default.txrns_div ZORDER BY (transaction_code)")
spark.sql(f"CACHE TABLE txrns_cache_div OPTIONS('storageLevel' 'DISK_ONLY') SELECT * FROM default.txrns_div;")
transactions_div = spark.read.table(f'txrns_cache_div')

transactions_div.select('ehhn').dropDuplicates().count()
transactions_div.limit(10).toPandas()
'''

# COMMAND ----------

#transactions_div.select('store_code').distinct().display()

# COMMAND ----------

# 04/09/2026: Commented out this code. We do not preserve markets with stores. this can be easily found in different data feeds. Only care about FC identified HHs.
'''
txrns = txrns_fc.union(txrns_div)

txrns.select('geo_sto_dsc_tx').distinct().display()

# Display results and ensure counts
if print_output == True:
  txrns.display()
  print("Total row count: ", txrns.count())
  print("Distinct count: ", txrns.select('ehhn').dropDuplicates().count())
'''

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delivery

# COMMAND ----------

# get delivery modality transactions with spend > 0

# 04/09/2026: Replaced transactions table (union of FC HHs and HHsin existing markets) with just FC HHs
delivery_txn = txrns_fc \
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

# 04/09/2026: Replaced transactions table (union of FC HHs and HHsin existing markets) with just FC HHs
pickup_txn = txrns_fc \
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

# Combine pickup and delivery to transactions and create 'tx_type' column for 
# identifying Ocado, Retail, Ecomm, PU Outlier, Del Outlier or other
# add ecomm and in-store vintages

# 04/09/2026: Replaced transactions table (union of FC HHs and HHsin existing markets) with just FC HHs
final_txn = txrns_fc.alias('a')\
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

final_txn.groupBy('tx_type').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reports

# COMMAND ----------

# MAGIC %md
# MAGIC #### Store

# COMMAND ----------

# 04/09/2026: Replaced transactions table (union of FC HHs and HHsin existing markets) with just FC HHs
basket_store = txrns_fc.alias('a')\
  .join(final_txn.alias('b'),'transaction_code')\
  .groupBy('a.store_code', 'a.geo_sto_dsc_tx','a.fiscal_week', 'b.tx_type')\
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
  basket_store.display()

if write_output:
  # store in bask_store_files directory
  create_named_csv(
    basket_store,
    path = wd+"/storeless_hhs_files",
    file_name = "bask_store_storeless",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  # add to  /send folder to send to nifi
  create_named_csv(
    basket_store,
    path = wd+"/send",
    file_name = "bask_store_storeless",
    fiscal_week = fiscal_week, 
    mode = mode
  )

# COMMAND ----------

# does every Ocado New Market have a row for every week in the given time period?
basket_store.groupBy('store_code').count().display()

# COMMAND ----------

#spark.sql(f'DROP TABLE IF EXISTS default.transactions_{run_id}')