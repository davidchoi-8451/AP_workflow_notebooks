# Databricks notebook source
# Run on shared cluster
from kayday import KrogerDate
from datetime import datetime
from effodata import ACDS
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark.sql.functions import to_date, date_format, coalesce, col

# COMMAND ----------

# params
wd = 'abfss://tmkrprtnrs-users@sa8451krprtnrdev.dfs.core.windows.net/l920466/ap-pos-feed'
dbutils.widgets.text(name = 'fiscal_week', label = 'Fiscal Date (YYYYPPWW)', defaultValue = '')
fiscal_date = dbutils.widgets.get('fiscal_week')
#dbutils.widgets.text(name = 'run_date', label = 'Run Date (YYYYPPWW)', defaultValue = '')
#run_date = dbutils.widgets.get('run_date')

# COMMAND ----------

acds = ACDS(use_sample_mart=True)
dates = acds.dates.select('fiscal_year', 'fiscal_week', 'fiscal_quarter_of_year_num', 'fiscal_month').distinct()

# COMMAND ----------

# Our File: SW Check numbers for Net Sales (net sales numbers are based on ALL storeless sales, not just from New HHs)
# Validate Net Sales and HH (total counts and trends) for the storeless markets 

# 23 - 26 Restatement: Change this path depending on what period we are validating

bask_sales_storeless_dir = f"/storeless_hhs_files/bask_store_storeless_20260101_20260704.csv"
bask_str_sales = spark.read.csv(wd + bask_sales_storeless_dir, header=True)

# COMMAND ----------

# Single Period Path (For monthly runs, run just this and comment out the union)
#bask_sales_storeless_dir_monthly = f"/storeless_hhs_files/bask_store_storeless_20260601_20260604.csv"
#bask_str_sales_single_period = spark.read.csv(wd + bask_sales_storeless_dir_monthly, header=True)

# Union bask_str_sales and bask_str_sales_single_period
#bask_str_sales_full = bask_str_sales.unionByName(bask_str_sales_single_period)
#display(bask_str_sales_full)

# COMMAND ----------

# import AP's comparison file from Azure
ap_comp_storeless = spark.read.csv(wd + f"/qa/{fiscal_date}/from-ap-storeless", header=True).withColumn(
  "week", date_format(coalesce( to_date(col("week"), "M/d/yyyy"), to_date(col("week"), "MM/dd/yyyy"), to_date(col("week"), "yyyy-MM-dd"), to_date(col("week"), "yyyy/MM/dd") ), "yyyy-MM-dd")) \
  .join(acds.dates.select('date', 'fiscal_week'), on = [f.col('week') == f.col('date')], how = 'inner') \
  .withColumnRenamed('Net Sales', 'net_sales_ap')\
  .withColumnRenamed('Visits', 'visits_ap')\
  .withColumnRenamed('HHs', 'hhs_ap')\
  .select('division', 'fiscal_week', 'net_sales_ap', 'visits_ap', 'hhs_ap')

ap_comp_storeless.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compare and Validate Net Sales and HH Count and Trend

# COMMAND ----------

# Get just basket report for Birmingham 
bask_str_sales_birmingham = (
    bask_str_sales
    .dropDuplicates()
    .withColumn('new_market', f.when(f.col('store_code') == '540_00042', f.lit('Storeless - Birmingham')).otherwise(f.lit('Exclude')))
    .join(dates, how='inner', on='fiscal_week')
    .filter(f.col('new_market') == 'Storeless - Birmingham')
    .withColumn('visits', f.col('visits').cast('float'))
    .withColumn('hhs', f.col('hhs').cast('float'))
    .withColumn('net_sales', f.col('net_sales').cast('float'))
    .select('fiscal_week', 'store_code', 'tx_type', 'visits', 'hhs', 'net_sales')
)
bask_str_sales_birmingham.display()

# COMMAND ----------

# Join AP and Our Basket report so that we can compare
birmingham_comp = bask_str_sales_birmingham.join(
    ap_comp_storeless, on='fiscal_week', how='inner'
)
display(birmingham_comp)

# COMMAND ----------

birmingham_comp_diff = (
    birmingham_comp  
    .withColumn('net_sales_diff', (f.col('net_sales_ap') - f.col('net_sales')))
    .withColumn('hhs_diff', (f.col('hhs_ap') - f.col('hhs')))
    .withColumn('max_diff', f.greatest('net_sales_diff', 'hhs_diff'))
    .withColumn('min_diff', f.least('net_sales_diff', 'hhs_diff'))
    .orderBy(f.desc(f.abs(f.col('max_diff'))), f.desc(f.abs(f.col('min_diff'))))
)

birmingham_comp_diff.display()

# Assert Error if the differences are greater than the threshold
error_flag = (
    birmingham_comp_diff
    .filter((f.col('max_diff') > 1) | (f.col('min_diff') < -1))
    .count()
)

assert error_flag == 0, \
    f"Data Validation Failed. Found {error_flag} where max/min exceeded"

# COMMAND ----------

# Output this data validation
birmingham_comp_diff.coalesce(1).write.mode('overwrite').csv(wd + f"/qa/{fiscal_date}/output_storeless", header = True)

# COMMAND ----------

storeless_df = spark.read.csv(wd + f"/qa/*/output_storeless", header=True)\
  .withColumn('net_sales', f.col('net_sales').cast('double'))\
  .withColumn('net_sales_ap', f.col('net_sales_ap').cast('double'))
storeless_df.display()
