# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Remove all widget parameters to reset the process

dbutils.widgets.removeAll()

# COMMAND ----------

# Import relevant libraries for data manipulation

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# Create the parameters specific for the notebook

dbutils.widgets.text('storageName', 'adlsjscgdevelopment')
dbutils.widgets.text('directory', 'ecommerce_sales_transformed')
dbutils.widgets.text('catalog', 'development')
dbutils.widgets.text('source_schema', 'bronze')
dbutils.widgets.text('sink_schema', 'silver')
dbutils.widgets.text('table', 'ecommerce_sales_transformed')

# COMMAND ----------

# Assign the parameters to recursive variables

storageName = dbutils.widgets.get('storageName')
directory  = dbutils.widgets.get('directory')
catalog = dbutils.widgets.get('catalog')
source_schema = dbutils.widgets.get('source_schema')
sink_schema = dbutils.widgets.get('sink_schema')
table = dbutils.widgets.get('table')

# COMMAND ----------

# Call the source tables as dataframes for manipulation and transformation

df_sales = spark.table(f'{catalog}.{source_schema}.ecommerce_sales')
df_customers = spark.table(f'{catalog}.{source_schema}.customer_master')
df_orders = spark.table(f'{catalog}.{source_schema}.order_items')
df_products = spark.table(f'{catalog}.{source_schema}.product_catalog')

# COMMAND ----------

# Generate a new df joining all source tables to get all the required information using left joins to then catch unmatched records and passed them into data quality logs for silver transformation

df_detailed_sales = df_sales.alias('sales').join(df_customers.alias('customers'), col('sales.customer_id') == col('customers.customer_id'), 'left')\
             .join(df_orders.alias('orders'), col('sales.order_id') == col('orders.order_id'), 'left')\
             .join(df_products.alias('products'), col('orders.product_id') == col('products.product_id'), 'left')

# Create the final dataframe with the required columns

df_final = df_detailed_sales.select(col('sales.order_id').alias('order_id'),
                                    col('sales.order_date').alias('order_date'),
                                    col('sales.order_time').alias('order_time'),
                                    col('sales.order_status').alias('order_status'),
                                    col('sales.sales_channel').alias('sales_channel'),
                                    col('sales.payment_method').alias('payment_method'),
                                    col('sales.payment_status').alias('payment_status'),
                                    col('sales.currency').alias('currency'),
                                    col('sales.shipping_method').alias('shipping_method'),
                                    col('sales.delivery_days').alias('delivery_days'),
                                    col('sales.estimated_delivery_days').alias('estimated_delivery_days'),
                                    col('sales.delivery_status').alias('delivery_status'),
                                    col('sales.customer_id').alias('customer_id'),
                                    col('sales.customer_name').alias('customer_name'),
                                    col('customers.customer_age').alias('customer_age'),
                                    col('customers.customer_segment').alias('customer_segment'),
                                    col('customers.customer_city').alias('customer_city'),
                                    col('customers.customer_state').alias('customer_state'),
                                    col('customers.customer_country').alias('customer_country'),
                                    col('customers.region').alias('customer_region'),
                                    col('customers.customer_acquisition_cost').alias('customer_acquisition_cost'),
                                    col('orders.product_id').alias('product_id'),
                                    col('orders.quantity').alias('quantity'),
                                    col('orders.unit_price').alias('unit_price'),
                                    col('orders.discount_percentage').alias('discount_percentage'),
                                    col('orders.discount_amount').alias('discount_amount'),
                                    col('orders.gross_sales').alias('gross_sales'),
                                    col('orders.tax_amount').alias('tax_amount'),
                                    col('orders.shipping_cost').alias('shipping_cost'),
                                    col('orders.net_sales').alias('net_sales'),
                                    col('orders.product_cost').alias('product_cost'),
                                    col('orders.profit').alias('profit'),
                                    col('products.product_name').alias('product_name'),
                                    col('products.product_category').alias('product_category'),
                                    col('products.product_subcategory').alias('product_subcategory'),
                                    col('products.brand').alias('product_brand'),
                                    col('products.supplier').alias('product_supplier'))


# COMMAND ----------

# Extraction of unmatched records for data quality logs

unmatched = (df_final.filter(col('product_name').isNull() | col('product_id').isNull() | col('customer_age').isNull())
    .select(lit("unmatched_join").alias("check_type"),
            concat_ws(" | ", col("order_id"), col("product_id")).alias("key_value"),
            lit("missing customer, order_items, or product_catalog match").alias("detail"),
            lit(f"{catalog}.{sink_schema}.{table}").alias("source_table"),
            lit(None).cast("string").alias("source_file"),
            lit(None).cast("string").alias("file_path"))
    .withColumn("detected_at", current_timestamp()))

if unmatched.limit(1).count() > 0:
    unmatched.write.mode("append").saveAsTable(f"{catalog}.{sink_schema}.dq_issues_log")

# COMMAND ----------

# Add age buckets for analysis

df_final = df_final.withColumn('age_bucket', when(col('customer_age') < 20, '0-19').when(col('customer_age') < 30, '20-29').when(col('customer_age') < 40, '30-39').when(col('customer_age') < 50, '40-49').when(col('customer_age') < 60, '50-59').when(col('customer_age') < 70, '60-69').when(col('customer_age') < 80, '70-79').when(col('customer_age') < 90, '80-89').when(col('customer_age') < 100, '90-99').otherwise('100+'))\
    .filter(col('product_id').isNotNull())\
    .dropDuplicates(['order_id', 'product_id'])

df_final.createOrReplaceTempView("df_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Define the structure and data types of the delta table to be populated
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.${sink_schema}.${table} (
# MAGIC   order_id STRING,
# MAGIC   order_date DATE,
# MAGIC   order_time STRING,
# MAGIC   order_status STRING,
# MAGIC   sales_channel STRING,
# MAGIC   payment_method STRING,
# MAGIC   payment_status STRING,
# MAGIC   currency STRING,
# MAGIC   shipping_method STRING,
# MAGIC   delivery_days DOUBLE,
# MAGIC   estimated_delivery_days DOUBLE,
# MAGIC   delivery_status STRING,
# MAGIC   customer_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   customer_age INT,
# MAGIC   age_bucket STRING,
# MAGIC   customer_segment STRING,
# MAGIC   customer_city STRING,
# MAGIC   customer_state STRING,
# MAGIC   customer_country STRING,
# MAGIC   customer_region STRING,
# MAGIC   customer_acquisition_cost DOUBLE,
# MAGIC   product_id STRING,
# MAGIC   quantity INT,
# MAGIC   unit_price DOUBLE,
# MAGIC   discount_percentage DOUBLE,
# MAGIC   discount_amount DOUBLE,
# MAGIC   gross_sales DOUBLE,
# MAGIC   tax_amount DOUBLE,
# MAGIC   shipping_cost DOUBLE,
# MAGIC   net_sales DOUBLE,
# MAGIC   product_cost DOUBLE,
# MAGIC   profit DOUBLE,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   product_subcategory STRING,
# MAGIC   product_brand STRING,
# MAGIC   product_supplier STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://${sink_schema}@${storageName}.dfs.core.windows.net/${directory}"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Upsert data into the delta table
# MAGIC
# MAGIC MERGE INTO ${catalog}.${sink_schema}.${table} AS TARGET
# MAGIC USING df_temp_view AS SOURCE
# MAGIC ON TARGET.order_id = SOURCE.order_id
# MAGIC   AND TARGET.product_id = SOURCE.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *