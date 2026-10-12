# Databricks notebook source
# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %md #### Srv Principal

# COMMAND ----------

# MAGIC %run /Shared/KrPrtnrs_service_principal

# COMMAND ----------

# MAGIC %md #### Libraries

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
from effodata import ACDS, golden_rules, effodata, Joiner, Sifter, Equality, Between
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, Rollup, Cube, available_metrics, get_metrics
from kayday import KrogerDate
import seg
import random
import string

# options
pd.set_option('display.max_columns', 100)
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.shuffle.partitions", "264")

# COMMAND ----------

# MAGIC %md #### Parameters

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

dbutils.widgets.text(
  name = "our_brands", 
  defaultValue = "False", 
  label = "our_brands")


# COMMAND ----------

# define working directory
wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'

# Set dates and variables:
db = 'l920466_cmm'
run_id = ''.join(random.choice(string.ascii_letters) for x in range(5))
start_date = dbutils.widgets.get('start_date')
end_date = dbutils.widgets.get('end_date')
fiscal_week = KrogerDate(date=end_date).format_week().replace("F", "")
start_week = KrogerDate(date=start_date).format_week().replace("F", "")
end_week = fiscal_week
mode = 'overwrite'
print_output = False
write_output = True
use_sample_mart = False
our_brands = dbutils.widgets.get('our_brands') == 'True'

# print parameters
print("run_id: " + run_id)
print("our_brands: " + str(our_brands))
print("start date: " + start_date)
print("end_date: " + end_date)
print("start_week: " + start_week)
print("end_week: " + end_week)
print("fiscal_week: " + fiscal_week)
print("write mode: " + mode)

# COMMAND ----------

# MAGIC %md #### UDFs

# COMMAND ----------

# Create a singular named csv file

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

# track how many records are processed each week by appending the current run's metadata to CSV file.

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

# MAGIC %md ### Data

# COMMAND ----------

# MAGIC %md #### ACDS

# COMMAND ----------

# transactions data store
acds = ACDS(use_sample_mart=use_sample_mart)
kpi = KPI(use_sample_mart=use_sample_mart)

# COMMAND ----------

# MAGIC %md #### Golden Rules

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

# MAGIC %md #### Transactions

# COMMAND ----------

# Get transactions data using the self-initialized start and end dates, and 
# apply previously initialized golden rules to exclude products and divisions

transactions = acds\
  .get_transactions(
    start_date = start_date,
    end_date = end_date, 
    apply_golden_rules = grs_new, 
    query_filters = ['gtin_no <> "PAPER KROGER"',
                      'gtin_no <> "PAPER MANUF."'],
    join_with=["dates", "products", "corporate_brands"], 
    join_type="left")\
  .select(
    'ehhn', 'cus_act_no', 'gtin_no', 'sto_no', 'mgt_div_no', 'promo_cycle_week', 
    'modality', 'transaction_code', 'trn_dt', 'trn_tm', 'store_code', 
    f.col('pid_fyt_rec_dpt_dsc_tx').alias('dept'), 'corporate_brands_brand',
    'net_spend_amt', 'fiscal_week', 'scn_unt_qy', 'loyalty_disc_amt', 'manufacturer_disc_amt', 
    'match_disc_amt', 'kroger_disc_amt', 'item_snap_flg', 'rtl_dol_am', 'item_wic_flg') \
      .withColumnRenamed('corporate_brands_brand', 'brand')

# # write delta table, optimize and cache table
# spark.sql(f'DROP TABLE IF EXISTS {db}.transactions_{run_id}')
# spark.sql(f'DROP TABLE IF EXISTS transactions_{run_id}_cache')
# txrns.write.format('delta').saveAsTable(f'{db}.transactions_{run_id}')
# spark.sql(f"OPTIMIZE {db}.transactions_{run_id} ZORDER BY (transaction_code)")
# spark.sql(f"CACHE TABLE transactions_{run_id}_cache OPTIONS('storageLevel' 'DISK_ONLY') SELECT * FROM {db}.transactions_{run_id};")
# transactions = spark.read.table(f'transactions_{run_id}_cache')

# if print_output:
#   transactions.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md #### Pickup

# COMMAND ----------

# pickup transactions less any of the outliers below
# households with 1 or more transactions in different divisions on the same date
# households with 4 or more visits on the same day
# households with transactions in more than 1 store or more than one card in the same date and time

# Transactions with "pickup" modality
pickup_txn = transactions\
  .filter((col('modality')=='PICKUP'))\
  .select('transaction_code','trn_dt','trn_tm','store_code','mgt_div_no','cus_act_no','ehhn')

# Outlier: Pickup transactions with multiple divisions on same trn_dt
pu_outlier_1 = pickup_txn\
  .groupBy('ehhn','trn_dt')\
  .agg(f.countDistinct('mgt_div_no').alias('num_div'))\
  .filter(col('num_div')>1)\
  .select('ehhn','trn_dt')

# Outlier: Pickup transactions with more than 4 visits on the same trn_dt
pu_outlier_2 = pickup_txn\
  .groupBy('ehhn','trn_dt')\
  .agg(f.countDistinct('transaction_code').alias('num_visits'))\
  .filter(col('num_visits')>4)\
  .select('ehhn','trn_dt')
  
# Outlier: Pickup transactions with multiple stores or multiple cards on the same trn_dt
pu_outlier_3 = pickup_txn\
  .groupBy('ehhn','trn_dt','trn_tm')\
  .agg(f.countDistinct('store_code').alias('count_stores'),
  f.countDistinct('cus_act_no').alias('count_cards'))\
  .filter((col('count_stores')>1)|(col('count_cards')>1))\
  .select('ehhn','trn_dt')

# Outlier dataframe that unions all type of outliers into one table
pu_outliers = pu_outlier_1.union(pu_outlier_2).union(pu_outlier_3)

# Mutually exclusive join to EXCLUDE all outliers from pickup transaction data
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

# MAGIC
# MAGIC %md #### Delivery

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

# Join back to delivery_txn to keep only rows where delivery transactions exceeded 0
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

# MAGIC %md #### Combine

# COMMAND ----------

# Combine pickup and delivery to transactions and create 'tx_type' column for 
# identifying Ocado, Retail, Ecomm, PU Outlier, Del Outlier or other
# add ecomm and in-store vintages

# div_no 540 is Ocado
# PICKUP + indicator == 1 is Ecomm
# DELIVERY + indicator == 1 is also Ecomm
# PICKUP + indicator is NULL is Pickup Outlier
# DELIVERY + indicator is NULL is Delivery Outlier
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

# MAGIC %md ### Reports

# COMMAND ----------

# MAGIC %md #### Store report

# COMMAND ----------

# Calculating KPIs via aggregation
measures = [
  f.countDistinct(f.when(col('scn_unt_qy')>0,col('a.transaction_code'))).alias('visits')
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
]

if our_brands:
  # our brands measure break outs
  basket_store = transactions.alias('a')\
    .join(final_txn.alias('b'),'transaction_code')\
    .filter(f.col('brand').isNotNull())\
    .withColumn(
      'brand', 
      f.when(f.col('brand') == 'PRIVATE SELECTION', 'ps')\
      .when(f.col('brand') == 'ST ORGANIC', 'st')\
      .when(f.col('brand') == 'SIMPLE TRUTH', 'st')\
      .when(f.col('brand') == 'BANNER', 'banner')\
      .when(~f.col('brand').isin('ps', 'st', 'banner'), 'other')\
      .otherwise('Error')
    )\
    .groupBy('a.mgt_div_no', 'a.store_code', 'a.fiscal_week', 'b.tx_type')\
    .pivot('brand')\
    .agg(*measures)\
    .filter(col('tx_type').isin('Retail','Ecomm','Ocado'))

else:
  # total basket only
  basket_store = transactions.alias('a')\
    .join(final_txn.alias('b'),'transaction_code')\
    .groupBy('a.mgt_div_no', 'a.store_code', 'a.fiscal_week', 'b.tx_type')\
    .agg(*measures)\
    .filter(col('tx_type').isin('Retail','Ecomm','Ocado'))

if print_output:
  print(f"{basket_store.count()} rows in basket_store")
  print("Sample 10 rows of basket_store: \n")
  basket_store.limit(10).display()

if write_output:
  
  #log_counts(basket_store, path=wd+"/bask_store_counts", fiscal_week = fiscal_week)
  # store in bask_store_files directory
  create_named_csv(
    basket_store,
    path = wd+"/bask_store_files",
    file_name = "our_brands_bask_store" if our_brands else "bask_store",
    fiscal_week = fiscal_week, 
    mode = mode
  )

 # add to  /send folder to send to nifi
  create_named_csv(
    basket_store,
    path = wd+"/send",
    file_name = "our_brands_bask_store" if our_brands else "bask_store",
    fiscal_week = fiscal_week, 
    mode = mode
  )

# COMMAND ----------

# MAGIC
# MAGIC %md #### Department report

# COMMAND ----------

# Changed below to left join on entire adweek data table. Prior logic that narrowed on start and end dates was cutting the first 3 days of the week.
# 20220211: Added fillna() on and and display flags and removed testing for isNull in each case statement.
# 20250207: Changed out the adweek table from ACDS to P&P's new table based on Purered data from Kroger

# COMMAND ----------

adweek =  spark.read.table("price_promotions_prd.science.effo_adweek_purered_v")

# COMMAND ----------

measures = [
  f.countDistinct(f.when(col('a.scn_unt_qy')>0,col('a.transaction_code'))).alias('visits')
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
]

if our_brands:
  # our brands measure break outs
  dept_store = transactions.alias('a')\
    .join(final_txn.alias('b'),'transaction_code')\
    .join(adweek.alias('g'),['promo_cycle_week','store_code','gtin_no'], how = "left")\
    .fillna(0, subset=['adv_typ_cd', 'dpl_typ_cd'])\
    .filter(f.col('brand').isNotNull())\
    .withColumn(
      'brand', 
      f.when(f.col('brand') == 'PRIVATE SELECTION', 'ps')\
      .when(f.col('brand') == 'ST ORGANIC', 'st')\
      .when(f.col('brand') == 'SIMPLE TRUTH', 'st')\
      .when(f.col('brand') == 'BANNER', 'banner')\
      .when(~f.col('brand').isin('ps', 'st', 'banner'), 'other')\
      .otherwise('Error')
    )\
    .groupBy('a.mgt_div_no', 'a.store_code', 'a.dept', 'a.fiscal_week', 'b.tx_type')\
    .pivot('brand')\
    .agg(*measures)\
    .filter(col('tx_type').isin('Retail','Ecomm','Ocado'))

else :
  dept_store = transactions.alias('a')\
    .join(final_txn.alias('b'),'transaction_code')\
    .join(adweek.alias('g'),['promo_cycle_week','store_code','gtin_no'], how = "left")\
    .fillna(0, subset=['adv_typ_cd', 'dpl_typ_cd'])\
    .groupBy('a.mgt_div_no', 'a.store_code', 'a.dept', 'a.fiscal_week', 'b.tx_type')\
    .agg(*measures)\
    .filter(col('tx_type').isin('Retail','Ecomm','Ocado'))


if print_output:
  print(f"{dept_store.count()} rows in dept_store")
  print("Sample 10 rows of dept_store: \n")
  dept_store.limit(10).display()

if write_output:
  #log_counts(dept_store, path=wd+"/dept_store_counts", fiscal_week=fiscal_week, mode = mode)
  #store in bask_store_files
  create_named_csv(
    dept_store,
    path = wd+"/dept_store_files",
    file_name = "our_brands_dept_store" if our_brands else "dept_store",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  # add to /send directory for send to nifi
  create_named_csv(
    dept_store,
    path = wd+"/send",
    file_name = "our_brands_dept_store" if our_brands else "dept_store",
    fiscal_week = fiscal_week, 
    mode = mode
  )

  if print_output:
    print(dept_store.select("*").count())
    print(dept_store.select("*").distinct().count())

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS default.transactions_{run_id}')