# Databricks notebook source
# MAGIC %md ## Validate Omni Updates

# COMMAND ----------

from kayday import KrogerDate
from datetime import datetime
from effodata import ACDS
import pyspark.sql.functions as f
from pyspark.sql.functions import to_date, date_format, coalesce, col

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
bask_str_dir = f"/bask_store_omni_files/bask_store_omni_*_{run_date}*.csv"
dept_str_dir = f"/dept_store_omni_files/dept_store_omni_*_{run_date}*.csv"
bask_str = spark.read.csv(wd + bask_str_dir, header=True)
dept_str = spark.read.csv(wd + dept_str_dir, header=True)

# aggregate to div week tx_type
bask_str_agg = bask_str\
  .dropDuplicates() \
  .groupBy('pref_div', 'fiscal_week', 'enterprise_seg')\
  .agg(
    f.sum('net_sales').alias('net_sales_source'), 
    f.sum('visits').alias('visits_source'))

dept_str_agg = dept_str\
  .dropDuplicates() \
  .groupBy('pref_div', 'fiscal_week', 'enterprise_seg')\
  .agg(
    f.sum('adonly_giveaway').alias('ad_only_source'),
    f.sum('displayonly_giveaway').alias('display_only_source'),
    f.sum('adwdisp_giveaway').alias('ad_w_display_source'), 
    f.sum('discountonly_giveaway').alias('discount_only_source'))

# join bask and dept
effo_source = bask_str_agg\
  .join(dept_str_agg, on = ['pref_div', 'fiscal_week', 'enterprise_seg'], how = 'left')

# vis
print(effo_source.select('pref_div', 'fiscal_week', 'enterprise_seg').count())
print(effo_source.select('pref_div', 'fiscal_week', 'enterprise_seg').distinct().count())

# test for dups
assert effo_source.select('pref_div', 'fiscal_week', 'enterprise_seg').count() == \
  effo_source.select('pref_div', 'fiscal_week', 'enterprise_seg').distinct().count(), "duplicate ids"

# COMMAND ----------

effo_source.display()

# COMMAND ----------

divisions = spark.read.csv(wd + "/division_lookup", header=True) \
  .withColumn('mgt_div_no', f.lpad(f.col('mgt_div_no'), 3, '0'))

# import comparison file from ap
ap = spark.read.csv(wd + f"/qa/{fiscal_date}/from-ap-omni", header=True, inferSchema=True)\
    .withColumn("week", date_format(coalesce( to_date(col("week"), "M/d/yyyy"), to_date(col("week"), "MM/dd/yyyy"), to_date(col("week"), "yyyy-MM-dd"), to_date(col("week"), "yyyy/MM/dd") ), "yyyy-MM-dd")) \
    .join(acds.dates.select('date', 'fiscal_week'), on = [f.col('week') == f.col('date')], how = 'inner') \
    .drop('date') \
  .withColumnRenamed('Net Sales', 'net_sales_ap')\
  .withColumnRenamed('Visits', 'visits_ap')\
  .withColumnRenamed('Giveaway - Ad Only', 'ad_only_ap')\
  .withColumnRenamed('Giveaway - Ad with Display', 'ad_w_display_ap')\
  .withColumnRenamed('Giveaway - Discount Only', 'discount_only_ap')\
  .withColumnRenamed('Giveaway - Display Only', 'display_only_ap')\
  .withColumnRenamed('pref_div', 'division')\
  .join(divisions, on = 'division', how = 'left')\
  .drop('week', 'division')

# vis
ap.limit(5).display()
print(divisions.count())
print(divisions.select('mgt_div_no').distinct().count())

# test
assert ap.filter(f.col('mgt_div_no').isNull()).count() == 0,\
  "missing divison number"

  #   .withColumn('fiscal_week', kro_date_udf(f.col('week')))\
  # .withColumn('fiscal_week', f.regexp_replace('fiscal_week', 'F', ''))\

# COMMAND ----------

bask_str = effo_source\
  .join(ap.withColumnRenamed('mgt_div_no', 'pref_div'), on = ['pref_div', 'fiscal_week', 'enterprise_seg'], how = 'inner')\
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

assert bask_str.filter((f.col('max_diff') > 1) | (f.col('min_diff') < -1)).count() == 0,\
  "max/min difference exceeded"

# COMMAND ----------

# create output
bask_str.coalesce(1).write.mode('overwrite').csv(wd + f"/qa/{fiscal_date}/output_omni", header = True)

# COMMAND ----------

# qa trends over time
df = spark.read.csv(wd + f"/qa/*/output_omni", header = True)\
  .withColumn('net_sales_source', f.col('net_sales_source').cast('double'))
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract

df = spark.read.csv(wd + f"/qa/*/output_omni", header=True) \
    .withColumn("file_path", col("_metadata.file_path")) \
    .withColumn("folder_name", regexp_extract(col("_metadata.file_path"), r"/qa/([^/]+)/output_omni", 1))

# COMMAND ----------

display(df.filter(f.col('fiscal_week') > "20250604").select('fiscal_week','folder_name').distinct())

# COMMAND ----------

