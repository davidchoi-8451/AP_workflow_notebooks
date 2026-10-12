# Databricks notebook source
# MAGIC %md ## Validate Period Updates

# COMMAND ----------

from kayday import KrogerDate
from datetime import datetime
from effodata import ACDS
import pyspark.sql.functions as f
import pandas as pd
import numpy as np
from pyspark.sql.functions import to_date, date_format, coalesce, col
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# params
acds = ACDS(use_sample_mart=True)

wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'
dbutils.widgets.text(name = 'fiscal_week', label = 'Fiscal Date (YYYYPPWW)', defaultValue = '')
dbutils.widgets.text(name = 'run_date', label = 'Run Date (YYYYMMDD)', defaultValue = '')
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
bask_str_dir = f"/bask_store_files/bask_store_{fiscal_date}_{run_date}*.csv"
dept_str_dir = f"/dept_store_files/dept_store_{fiscal_date}_{run_date}*.csv"
bask_str = spark.read.csv(wd + bask_str_dir, header=True)
dept_str = spark.read.csv(wd + dept_str_dir, header=True)

# aggregate to div week tx_type
bask_str_agg = bask_str\
  .groupBy('mgt_div_no', 'fiscal_week', 'tx_type')\
  .agg(
    f.sum('net_sales').alias('net_sales_source'), 
    f.sum('visits').alias('visits_source'))

dept_str_agg = dept_str\
  .groupBy('mgt_div_no', 'fiscal_week', 'tx_type')\
  .agg(
    f.sum('adonly_giveaway').alias('ad_only_source'),
    f.sum('displayonly_giveaway').alias('display_only_source'),
    f.sum('adwdisp_giveaway').alias('ad_w_display_source'), 
    f.sum('discountonly_giveaway').alias('discount_only_source'))

# join bask and dept
effo_source = bask_str_agg\
  .join(dept_str_agg, on = ['mgt_div_no', 'fiscal_week', 'tx_type'], how = 'left')\
  .withColumnRenamed('tx_type', 'channel')

# vis
print(effo_source.select('mgt_div_no', 'fiscal_week', 'channel').count())
print(effo_source.select('mgt_div_no', 'fiscal_week', 'channel').distinct().count())

# test for dups
assert effo_source.select('mgt_div_no', 'fiscal_week', 'channel').count() == \
  effo_source.select('mgt_div_no', 'fiscal_week', 'channel').distinct().count(), "duplicate ids"

# COMMAND ----------

effo_source.display()

# COMMAND ----------

divisions = spark.read.csv(wd + "/division_lookup", header=True) \
  .withColumn('mgt_div_no', f.lpad(f.col('mgt_div_no'), 3, '0')) \
  .withColumn('division',f.lower(f.col('division')))

ap = spark.read.csv(wd + f"/qa/{fiscal_date}/from-ap", header=True)
ap.display()

# import comparison file from ap
ap = (
    spark.read.csv(f"{wd}/qa/{fiscal_date}/from-ap", header=True)
    .withColumn(
        "parsed_date",
        f.coalesce(
            f.to_date(f.trim(f.col("week"))),
            f.to_date(f.trim(f.col("week")), "MM/dd/yyyy"),
            f.to_date(f.trim(f.col("week")), "M/d/yyyy"),
            f.to_date(f.trim(f.col("week")), "yyyy/MM/dd")
        )
    )
    .join(
        acds.dates.select("date", "fiscal_week"),
        on=[f.col("parsed_date") == f.col("date")],
        how="inner"
    )
    .drop("date", "parsed_date", "week")
    .withColumnRenamed("Division", "division")
    .withColumnRenamed("Type", "channel")
    .withColumnRenamed("Net Sales", "net_sales_ap")
    .withColumnRenamed("Visits", "visits_ap")
    .withColumnRenamed("Giveaway - Ad Only", "ad_only_ap")
    .withColumnRenamed("Giveaway - Ad with Display", "ad_w_display_ap")
    .withColumnRenamed("Giveaway - Discount Only", "discount_only_ap")
    .withColumnRenamed("Giveaway - Display Only", "display_only_ap")
    .withColumn("division", f.lower(f.trim(f.col("division"))))
    .join(divisions, on="division", how="left")
    .drop("division")
)

# vis
ap.limit(5).display()
print(ap.select('fiscal_week').distinct().count())
print(divisions.count())
print(divisions.select('mgt_div_no').distinct().count())

# test
assert ap.filter(f.col('mgt_div_no').isNull()).count() == 0,\
  "missing divison number"

  # .withColumn('fiscal_week', kro_date_udf(f.col('week')))\
  # .withColumn('fiscal_week', f.regexp_replace('fiscal_week', 'F', ''))\

# COMMAND ----------

bask_str = effo_source.join(ap, on = ['mgt_div_no', 'fiscal_week', 'channel'], how = 'inner')\
  .withColumn('net_sales_diff', (f.col('net_sales_ap') - f.col('net_sales_source')))\
  .withColumn('visits_diff', (f.col('visits_ap') - f.col('visits_source')))\
  .withColumn('ad_only_diff', (f.col('ad_only_ap') - f.col('ad_only_source')))\
  .withColumn('ad_w_display_diff', (f.col('ad_w_display_ap') - f.col('ad_w_display_source')))\
  .withColumn('discount_only_diff', (f.col('discount_only_ap') - f.col('discount_only_source')))\
  .withColumn('display_only_diff', (f.col('display_only_ap') - f.col('display_only_source')))\
  .withColumn('max_diff', f.greatest(
    'net_sales_diff', 'visits_diff', 'ad_only_diff', 
    'ad_w_display_diff', 'discount_only_diff', 'display_only_diff'))\
  .withColumn('min_diff', f.least(
    'net_sales_diff', 'visits_diff', 'ad_only_diff', 
    'ad_w_display_diff', 'discount_only_diff', 'display_only_diff'))\
  .orderBy('max_diff', 'min_diff') 

bask_str.display()

assert bask_str.filter((f.col('max_diff') > 1.2) | (f.col('min_diff') < -1.2)).count() == 0,\
  "max/min difference exceeded"

# COMMAND ----------

# create output
bask_str.coalesce(1).write.mode('overwrite').csv(wd + f"/qa/{fiscal_date}/output", header = True)

# COMMAND ----------

# qa trends over time
df = spark.read.csv(wd + f"/qa/*/output", header = True)\
  .withColumn('net_sales_source', f.col('net_sales_source').cast('double'))
display(df)

# COMMAND ----------

