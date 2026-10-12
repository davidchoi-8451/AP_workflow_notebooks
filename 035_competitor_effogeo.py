# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC %run /Shared/KrPrtnrs_service_principal

# COMMAND ----------

# MAGIC
# MAGIC %run "./file_utils_nb"

# COMMAND ----------

# MAGIC %pip install effo-geo

# COMMAND ----------

# MAGIC %md ### Import

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField
from kayday import KrogerDate
from datetime import datetime
from effodata import ACDS
import pandas as pd
from functools import reduce
from operator import add
acds = ACDS()

# COMMAND ----------

# MAGIC %md ### Parameters

# COMMAND ----------

# dates (USE FOR SINGLE PERIOD)

today = datetime.strftime(datetime.today(), '%Y-%m-%d')
period_ago = KrogerDate(date = today).ago(periods=1)
fiscal_month = str(period_ago.year) + str(period_ago.period).zfill(2)
last_period_date = str(
  acds.dates\
  .filter(f.col('fiscal_month') == fiscal_month)\
  .agg(f.max('date')).collect()[0][0])

print(fiscal_month)
print(last_period_date)


# COMMAND ----------

# USE FOR RESTATEMENT

# # 5-29 David Fix: Get target period instead of current period - 1
# target_date_str = dbutils.widgets.get("date") 

# anchor_date = KrogerDate(date = target_date_str)

# period_ago = anchor_date.ago(periods=0)
# fiscal_month = str(period_ago.year) + str(period_ago.period).zfill(2)
# last_period_date = str(
#      acds.dates\
#      .filter(f.col('fiscal_month') == fiscal_month)\
#      .agg(f.max('date')).collect()[0][0]
# )

# print(f"Target Input Date: {target_date_str}") 
# print(f"Prior Fiscal Period: {fiscal_month}")
# print(f"Calculated End Date: {last_period_date}")

# COMMAND ----------

#Divisions
divs = [
  "011","014","016","018","021","024","025","026","029","034",
  "035","531","534","615","620","660","701","703","705","706"
]

# directories
wd = "abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed"

# COMMAND ----------

# MAGIC %md ### Functions

# COMMAND ----------

def create_named_csv(sdf, path, file_name, fiscal_week):
  # create a named csv file from dataframe
  sdf\
    .coalesce(1)\
    .write\
    .mode('overwrite')\
    .csv(path+'/temp_files', header=True)

  # copy temp files to named file
  data_location = path+'/temp_files'
  files = dbutils.fs.ls(data_location)
  csv_file = [x.path for x in files if x.path.endswith('.csv')][0]
  dbutils.fs.cp(
    csv_file, 
    (path + "/" + file_name + "_" + fiscal_week + "_" + \
     format(datetime.now(), "%Y%m%d_%H%M%S") + '.csv'))
  dbutils.fs.rm(data_location, recurse=True) 

# COMMAND ----------

# MAGIC %md ### Get Competitors

# COMMAND ----------

from effogeo import Geo
gd = Geo()

# COMMAND ----------

# List of prefixes to check
comp_list = ["albertsons_count", "aldi_count", "costco_count", "food_lion_count", "heb_count", "jewel_count", "meijer_count", "piggly_wiggly_count", "publix_count", "safeway_count", "sams_club_count", "target_count", "trader_joes_count", "vons_count", "walgreens_count", "walmart_count", "whole_foods_count", "cvs_count"]

# Run Competitive Landscape with default settings
landscape, competitors =  gd.competitive_landscape(trade_channels=['01','03','07','08'],
                                                  sub_channels=["011","051","052","053","055","056","057"],
                                                  pivot_column='BANNER',
                                                  include_independents=False,
                                                  tdlinx_date = last_period_date,
                                                  use_acds=True)
                                                  
# Determine which secondary Walgreens column is available
if 'WALGREENS_CO_Count' in landscape.columns:
    walgreens_secondary_col = 'WALGREENS_CO_Count'
elif 'WALGREENS_BROOKSHIRE_Count' in landscape.columns:
    walgreens_secondary_col = 'WALGREENS_BROOKSHIRE_Count'
else:
    raise ValueError("Neither 'WALGREENS_CO_Count' nor 'WALGREENS_BROOKSHIRE_Count' found in landscape columns.")

# Select the filtered columns
count_comp = landscape \
  .withColumn('division', f.concat(f.col('store_code').substr(1, 3))) \
    .withColumn('date', f.lit(last_period_date)) \
    .filter(f.col('division').isin(divs)) \
      .withColumn('albertsons_count', (f.col('ALBERTSONS_Count') + f.col('ALBERTSONS_MARKET_Count') + f.col('ALBERTSONS_SAV_ON_Count'))) \
        .withColumnRenamed('ALDI_FOOD_STORE_Count', 'aldi_count') \
          .withColumnRenamed('COSTCO_WHOLESALE_Count', 'costco_count') \
            .withColumnRenamed('FOOD_LION_Count', 'food_lion_count') \
              .withColumn('heb_count', (f.col('H_E_B_Count') + f.col('H_E_B_PLUS_Count') + f.col('H_E_B_CENTRAL_MARKET_Count'))) \
                .withColumnRenamed('JEWEL_OSCO_Count', 'jewel_count') \
                  .withColumn('meijer_count', (f.col('MEIJER_Count') + f.col('MEIJER_INC_Count'))) \
                    .withColumnRenamed('PIGGLY_WIGGLY_Count', 'piggly_wiggly_count') \
                      .withColumnRenamed('PUBLIX_Count', 'publix_count') \
                        .withColumn('safeway_count', f.col('SAFEWAY_Count') + f.col('TOM_THUMB_SAFEWAY_Count') + f.col('SAFEWAY_FOODS_Count') + f.col('ALBERTSONS_SAFEWAY_INTER_MTN_DIV_Count')) \
                          .withColumnRenamed('SAMS_CLUB_Count', 'sams_club_count') \
                            .withColumn('target_count', f.col('TARGET_Count') + f.col('SUPERTARGET_Count') + f.col('TARGET_EXPRESS_Count') + f.col('TARGET_PFRESH_Count')) \
                              .withColumn('walmart_count', f.col('WALMART_NEIGHBORHOOD_MARKET_Count') + f.col('WALMART_STORE_Count') + f.col('WALMART_SUPERCENTER_Count')) \
                              .withColumnRenamed('ALBERTSONS_CO_PHARMACY_DIV_Count', 'albertsons_pharmacy_count') \
                              .withColumnRenamed('TRADER_JOES_Count', 'trader_joes_count') \
                                .withColumnRenamed('WHOLE_FOODS_Count', 'whole_foods_count') \
                                .withColumnRenamed('VONS_Count', 'vons_count') \
                                 .withColumn('walgreens_count', f.col('WALGREENS_Count') + f.col(walgreens_secondary_col)) \
                                      .withColumn('cvs_count', (f.col('CVS_PHARMACY_Count') + f.col('CVS_PHARMACY_SCHNUCKS_Count')+ f.col('CVS_PHARMACY_TARGET_Count') + f.col('CAREPLUS_CVS_PHARMACY_Count'))) \
                                        .select('store_code', 'date',*comp_list) \
                                        .withColumn('total_competitors', reduce(add, [f.col(x) for x in comp_list]))

# COMMAND ----------

# 2273, 20546
store_count = count_comp.select('store_code').distinct().count()
total_competitors_sum = count_comp.agg(f.sum('total_competitors')).first()[0]

print(store_count)
print(total_competitors_sum)

# COMMAND ----------

# Look at a couple counts to check for irregularities:
store_count = count_comp.select('store_code').distinct().count()
date_fmt = datetime.strftime(datetime.strptime(last_period_date, '%Y-%m-%d'), '%Y%m%d')


# store in /competitor_files folder
create_named_csv(
  count_comp,
  path = wd+"/competitor_files",
  file_name = "competitors",
  fiscal_week = date_fmt)

# add to /send folder to send to nifi
create_named_csv(
  count_comp,
  path = wd+"/send",
  file_name = "competitors",
  fiscal_week = date_fmt)


print(date_fmt)
print(store_count)