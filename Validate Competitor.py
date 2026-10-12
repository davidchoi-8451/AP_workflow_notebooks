# Databricks notebook source
from kayday import KrogerDate
from datetime import datetime
from effodata import ACDS
import pyspark.sql.functions as f
import pandas as pd
import numpy as np

# COMMAND ----------

# params

# sample mart (?) 
# if store closes down and a new one appears for example, diff would be 1 in the data (but true count is 13)
acds = ACDS(use_sample_mart=True)

# Run date will be second date in the most recent file in competitor-files
wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'
dbutils.widgets.text(name = 'fiscal_week', label = 'Fiscal Date (YYYYPPWW)', defaultValue = '')
dbutils.widgets.text(name = 'run_date', label = 'Run Date (YYYYPPWW)', defaultValue = '')
fiscal_date = dbutils.widgets.get('fiscal_week')
run_date = dbutils.widgets.get('run_date')

# COMMAND ----------

# # udf
# def kro_date(date): 
#   x = KrogerDate(date=datetime.strptime(date, '%Y-%m-%d')).format_week() 
#   return(x) 

# kro_date_udf = udf(lambda date: kro_date(date), f.StringType()) 

# COMMAND ----------

# import source
comp_str_dir = f"/competitor_files/competitors_*_{run_date}_*.csv"
comp_str = spark.read.csv(wd + comp_str_dir, header=True)

# aggregate to div week tx_type
effo_source = comp_str\
  .withColumn('mgt_div_no', f.substring(f.col('store_code'), 1, 3))\
    .groupBy('mgt_div_no', 'date') \
  .agg(
    f.sum('albertsons_count').alias('Albertsons_source'), 
    f.sum('aldi_count').alias('Aldi_source'),
    f.sum('costco_count').alias('Costco_source'),
    f.sum('food_lion_count').alias('Food_Lion_source'),
    f.sum('heb_count').alias('HEB_source'),
    f.sum('jewel_count').alias('Jewel_source'),
    f.sum('meijer_count').alias('Meiger_source'),
    f.sum('piggly_wiggly_count').alias('Piggly_Wiggly_source'),
    f.sum('publix_count').alias('Publix_source'),
    f.sum('safeway_count').alias('Safeway_source'),
    f.sum('sams_club_count').alias('Sams_Club_source'),
    f.sum('target_count').alias('Target_source'),
    f.sum('trader_joes_count').alias('Trader_Joes_source'),
    f.sum('vons_count').alias('Vons_source'),
    f.sum('walgreens_count').alias('Walgreens_source'),
    f.sum('walmart_count').alias('Walmart_source'),
    f.sum('whole_foods_count').alias('Whole_Foods_source'),
    f.sum('cvs_count').alias('CVS_source'),
    f.sum('total_competitors').alias('Total_Competitors_source')
  )

# vis
print(effo_source.select('mgt_div_no', 'date').count())
print(effo_source.select('mgt_div_no', 'date').distinct().count())

# test for dups
assert effo_source.select('mgt_div_no', 'date').count() == \
  effo_source.select('mgt_div_no', 'date').distinct().count(), "duplicate ids"

effo_source.display()

# COMMAND ----------

divisions = spark.read.csv(wd + "/division_lookup", header=True) \
  .withColumn('mgt_div_no', f.lpad(f.col('mgt_div_no'), 3, '0'))

divisions.display()

# COMMAND ----------


divisions = spark.read.csv(wd + "/division_lookup", header=True) \
  .withColumn('mgt_div_no', f.lpad(f.col('mgt_div_no'), 3, '0'))

# import comparison file from ap

#ap = spark.read.csv(wd + f"/qa/20251304/from-ap-comp", header=True)\
#  .withColumnRenamed('Division', 'division')\
#    .filter('period like "%P13%"') \

# Every time we run, Change this date to the date in which we are running qa for 
ap = spark.read.csv(wd + f"/qa/20260704/from-ap-comp", header=True)\
  .withColumnRenamed('Division', 'division')\
    .withColumnRenamed('Albertsons', 'Albertsons_ap')\
    .withColumnRenamed('Aldi','Aldi_ap')\
    .withColumnRenamed('Costco','Costco_ap')\
    .withColumnRenamed('Food Lion','Food_Lion_ap')\
    .withColumnRenamed('HEB','HEB_ap')\
    .withColumnRenamed('Jewel','Jewel_ap')\
    .withColumnRenamed('Meijer','Meijer_ap')\
    .withColumnRenamed('Piggy Wiggly','Piggly_Wiggly_ap')\
    .withColumnRenamed('Publix','Publix_ap')\
    .withColumnRenamed('Safeway','Safeway_ap')\
    .withColumnRenamed('Sams Club','Sams_Club_ap')\
    .withColumnRenamed('Target','Target_ap')\
    .withColumnRenamed('Trader Joes','Trader_Joes_ap')\
    .withColumnRenamed('Vons','Vons_ap')\
    .withColumnRenamed('Walgreens','Walgreens_ap')\
    .withColumnRenamed('Walmart','Walmart_ap')\
    .withColumnRenamed('Whole Foods','Whole_Foods_ap')\
    .withColumnRenamed('Rite Aid','RiteAid_ap')\
    .withColumnRenamed('CVS','CVS_ap')\
  .join(divisions, on = 'division', how = 'left').dropDuplicates()
    #.drop('division') \


ap.display()
# print(ap.select('date').distinct().count())
print(divisions.count())
print(divisions.select('mgt_div_no').distinct().count())

# # test
# assert ap.filter(f.col('mgt_div_no').isNull()).count() == 0,\
#   "missing divison number"

  # .withColumn('fiscal_week', kro_date_udf(f.col('week')))\
  # .withColumn('fiscal_week', f.regexp_replace('fiscal_week', 'F', ''))\

# COMMAND ----------

ap.orderBy('week', 'mgt_div_no').select('mgt_div_no', *[col for col in ap.columns if col != 'mgt_div_no']).display()

# COMMAND ----------

ap = ap.withColumn('date', f.to_date(f.col('week'), 'M/d/yyyy'))

# Join on div and date for restatement
#comp_str = effo_source.join(ap, on = ['mgt_div_no', 'date'], how = 'inner')\

comp_str = effo_source.join(ap, on = ['mgt_div_no', 'date'], how = 'inner')\
  .withColumn('Albertsons_diff', (f.col('Albertsons_ap') - f.col('Albertsons_source')))\
  .withColumn('Aldi_diff', (f.col('Aldi_ap') - f.col('Aldi_source')))\
  .withColumn('Costco_diff', (f.col('Costco_ap') - f.col('Costco_source')))\
  .withColumn('Food_Lion_diff', (f.col('Food_Lion_ap') - f.col('Food_Lion_source')))\
  .withColumn('HEB_diff', (f.col('HEB_ap') - f.col('HEB_source')))\
  .withColumn('Jewel_diff', (f.col('Jewel_ap') - f.col('Jewel_source')))\
  .withColumn('Meijer_diff', (f.col('Meijer_ap') - f.col('Meiger_source')))\
  .withColumn('Piggly_Wiggly_diff', (f.col('Piggly_Wiggly_ap') - f.col('Piggly_Wiggly_source')))\
  .withColumn('Publix_diff', (f.col('Publix_ap') - f.col('Publix_source')))\
  .withColumn('Safeway_diff', (f.col('Safeway_ap') - f.col('Safeway_source')))\
  .withColumn('Sams_Club_diff', (f.col('Sams_Club_ap') - f.col('Sams_Club_source')))\
  .withColumn('Target_diff', (f.col('Target_ap') - f.col('Target_source')))\
  .withColumn('Trader_Joes_diff', (f.col('Trader_Joes_ap') - f.col('Trader_Joes_source')))\
  .withColumn('Vons_diff', (f.col('Vons_ap') - f.col('Vons_source')))\
  .withColumn('Walgreens_diff', (f.col('Walgreens_ap') - f.col('Walgreens_source')))\
  .withColumn('Walmart_diff', (f.col('Walmart_ap') - f.col('Walmart_source')))\
  .withColumn('Whole_Foods_diff', (f.col('Whole_Foods_ap') - f.col('Whole_Foods_source')))\
  .withColumn('CVS_diff', (f.col('CVS_ap') - f.col('CVS_source')))\
  .withColumn('max_diff', f.greatest(
    'Albertsons_diff', 'Aldi_diff', 'Costco_diff', 'Food_Lion_diff', 'HEB_diff', 'Jewel_diff', 'Meijer_diff', 
    'Piggly_Wiggly_diff', 'Publix_diff', 'Safeway_diff', 'Sams_Club_diff', 'Target_diff', 'Trader_Joes_diff', 
    'Vons_diff', 'Walgreens_diff', 'Walmart_diff', 'Whole_Foods_diff', 'CVS_diff'))\
  .withColumn('min_diff', f.least(
    'Albertsons_diff', 'Aldi_diff', 'Costco_diff', 'Food_Lion_diff', 'HEB_diff', 'Jewel_diff', 'Meijer_diff', 
    'Piggly_Wiggly_diff', 'Publix_diff', 'Safeway_diff', 'Sams_Club_diff', 'Target_diff', 'Trader_Joes_diff', 
    'Vons_diff', 'Walgreens_diff', 'Walmart_diff', 'Whole_Foods_diff', 'CVS_diff'))\
   .orderBy(f.col('mgt_div_no').asc(), f.col('max_diff'), f.col('min_diff'))

#comp_str.display()

cols = ['mgt_div_no', 'period', 'date', 'week'] + [col for col in comp_str.columns if col not in ['mgt_div_no', 'period', 'date', 'week']]
comp_str.select(*cols).display()

assert comp_str.filter((f.col('max_diff') > 1.2) | (f.col('min_diff') < -1.2)).count() == 0,\
  "max/min difference exceeded"



# COMMAND ----------

ap_p5_2026 = spark.read.csv(wd + f"/qa/20260704/from-ap-comp", header=True)\
  .withColumnRenamed('Division', 'division')\
    .withColumnRenamed('Albertsons', 'Albertsons_ap')\
    .withColumnRenamed('Aldi','Aldi_ap')\
    .withColumnRenamed('Costco','Costco_ap')\
    .withColumnRenamed('Food Lion','Food_Lion_ap')\
    .withColumnRenamed('HEB','HEB_ap')\
    .withColumnRenamed('Jewel','Jewel_ap')\
    .withColumnRenamed('Meijer','Meijer_ap')\
    .withColumnRenamed('Piggy Wiggly','Piggly_Wiggly_ap')\
    .withColumnRenamed('Publix','Publix_ap')\
    .withColumnRenamed('Safeway','Safeway_ap')\
    .withColumnRenamed('Sams Club','Sams_Club_ap')\
    .withColumnRenamed('Target','Target_ap')\
    .withColumnRenamed('Trader Joes','Trader_Joes_ap')\
    .withColumnRenamed('Vons','Vons_ap')\
    .withColumnRenamed('Walgreens','Walgreens_ap')\
    .withColumnRenamed('Walmart','Walmart_ap')\
    .withColumnRenamed('Whole Foods','Whole_Foods_ap')\
    .withColumnRenamed('Rite Aid','RiteAid_ap')\
    .withColumnRenamed('CVS','CVS_ap')

ap_p5_2026.display()

effo_source.display()

# COMMAND ----------

# create output
comp_str.coalesce(1).write.mode('overwrite').csv(wd + f"/qa/20260704/output_comp", header = True)