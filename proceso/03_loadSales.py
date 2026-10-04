# Databricks notebook source
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
dbutils.widgets.text('directory', 'ecommerce_sales_summary')
dbutils.widgets.text('catalog', 'development')
dbutils.widgets.text('source_schema', 'silver')
dbutils.widgets.text('sink_schema', 'gold')
dbutils.widgets.text('table', 'ecommerce_sales_summary')

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

df_sales = spark.table(f'{catalog}.{source_schema}.ecommerce_sales_transformed')

# COMMAND ----------

# Generate a new df aggregating data per main analytics attributes, only completed orders

df_aggregated = df_sales.filter(col('order_status') == 'Completed').groupBy([col('order_date'),
                                                                             col('sales_channel'),
                                                                             col('payment_status'),
                                                                             col('currency'),
                                                                             col('delivery_status'),
                                                                             col('age_bucket'),
                                                                             col('customer_segment'),
                                                                             col('customer_country'),
                                                                             col('product_category'),
                                                                             col('product_subcategory'),
                                                                             col('product_brand'),
                                                                             col('product_supplier')])\
                                                                    .agg(sum(col('quantity')).alias('quantity'),
                                                                         sum(col('discount_amount')).alias('discount'),
                                                                         sum(col('gross_sales')).alias('gross_sales'),
                                                                         sum(col('tax_amount')).alias('tax_amount'),
                                                                         sum(col('shipping_cost')).alias('shipping_cost'),
                                                                         sum(col('net_sales')).alias('net_sales'),
                                                                         sum(col('product_cost')).alias('product_cost'),
                                                                         sum(col('profit')).alias('profit'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Define the structure and data types of the delta table to be populated
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.${sink_schema}.${table} (
# MAGIC   order_date DATE,
# MAGIC   sales_channel STRING,
# MAGIC   payment_status STRING,
# MAGIC   currency STRING,
# MAGIC   delivery_status STRING,
# MAGIC   age_bucket STRING,
# MAGIC   customer_segment STRING,
# MAGIC   customer_country STRING,
# MAGIC   product_category STRING,
# MAGIC   product_subcategory STRING,
# MAGIC   product_brand STRING,
# MAGIC   product_supplier STRING,
# MAGIC   quantity INT,
# MAGIC   discount DOUBLE,
# MAGIC   gross_sales DOUBLE,
# MAGIC   tax_amount DOUBLE,
# MAGIC   shipping_cost DOUBLE,
# MAGIC   net_sales DOUBLE,
# MAGIC   product_cost DOUBLE,
# MAGIC   profit DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://${sink_schema}@${storageName}.dfs.core.windows.net/${directory}"

# COMMAND ----------

# Insertion of data into final table ensuring the order does not break

df_aggregated.select(
    "order_date", "sales_channel", "payment_status", "currency", "delivery_status",
    "age_bucket", "customer_segment", "customer_country", "product_category",
    "product_subcategory", "product_brand", "product_supplier",
    "quantity", "discount", "gross_sales", "tax_amount", "shipping_cost",
    "net_sales", "product_cost", "profit"
).write.mode("overwrite").insertInto(f"{catalog}.{sink_schema}.{table}")