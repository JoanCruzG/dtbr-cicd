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
dbutils.widgets.text('container', 'raw')
dbutils.widgets.text('directory', 'ecommerce_sales')
dbutils.widgets.text('catalog', 'development')
dbutils.widgets.text('schema', 'bronze')
dbutils.widgets.text('table', 'ecommerce_sales')

# COMMAND ----------

# Assign the parameters to recursive variables

storageName = dbutils.widgets.get('storageName')
container = dbutils.widgets.get('container')
directory  = dbutils.widgets.get('directory')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
table = dbutils.widgets.get('table')

# Route of the container and directory where raw data is uploaded
route = f'abfss://{container}@{storageName}.dfs.core.windows.net/{directory}'

# Path to store the checkpoint and schema for AutoLoader ingestion (controls which raw files have already been processed using checkpoint log and schema evolution log)
checkpoint_path = f'abfss://checkpoints@{storageName}.dfs.core.windows.net/{directory}/_checkpoints'
schema_path = f'abfss://checkpoints@{storageName}.dfs.core.windows.net/{directory}/_schemas'

# Define the key columns of the raw data ingested

key_cols = ['order_id']

# COMMAND ----------

# Define a structure and data types for the raw data to be ingested

df_schema = StructType(fields=[StructField('order_id', StringType(), False),
            StructField('order_date', DateType(), True),
            StructField('order_time', StringType(), True),
            StructField('order_status', StringType(), True),
            StructField('sales_channel', StringType(), True),
            StructField('customer_id', StringType(), True),
            StructField('customer_name', StringType(), True),
            StructField('customer_age', IntegerType(), True),
            StructField('gender', StringType(), True),
            StructField('customer_segment', StringType(), True),
            StructField('customer_type', StringType(), True),
            StructField('customer_city', StringType(), True),
            StructField('customer_state', StringType(), True),
            StructField('customer_country', StringType(), True),
            StructField('region', StringType(), True),
            StructField('customer_postal_code', IntegerType(), True),
            StructField('payment_method', StringType(), True),
            StructField('payment_status', StringType(), True),
            StructField('currency', StringType(), True),
            StructField('shipping_method', StringType(), True),
            StructField('warehouse', StringType(), True),
            StructField('delivery_days', DoubleType(), True),
            StructField('estimated_delivery_days', DoubleType(), True),
            StructField('delivery_status', StringType(), True),
            StructField('return_status', StringType(), True),
            StructField('return_reason', StringType(), True),
            StructField('customer_rating', DoubleType(), True),
            StructField('review_sentiment', StringType(), True),
            StructField('customer_review', StringType(), True),
            StructField('marketing_channel', StringType(), True),
            StructField('campaign_name', StringType(), True),
            StructField('coupon_code', StringType(), True),
            StructField('loyalty_points_earned', IntegerType(), True),
            StructField('loyalty_points_redeemed', IntegerType(), True),
            StructField('quantity', IntegerType(), True),
            StructField('gross_sales', DoubleType(), True),
            StructField('discount_amount', DoubleType(), True),
            StructField('tax_amount', DoubleType(), True),
            StructField('shipping_cost', DoubleType(), True),
            StructField('net_sales', DoubleType(), True),
            StructField('product_cost', DoubleType(), True),
            StructField('profit', DoubleType(), True),
            StructField('profit_margin_percentage', DoubleType(), True),
            StructField('customer_lifetime_value', DoubleType(), True),
            StructField('is_repeat_customer', BooleanType(), True),
            StructField('customer_order_count', IntegerType(), True)
])

# COMMAND ----------

# Read data into a dataframe employing the previously defined structure using AutoLoader

df = spark.readStream.format('cloudFiles')\
                      .option('cloudFiles.format', 'csv')\
                      .option('cloudFiles.schemaLocation', schema_path)\
                      .option('header', True)\
                      .option('rescuedDataColumn', '_rescued_data')\
                      .schema(df_schema)\
                      .load(route)\
                      .selectExpr("*", "_metadata")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Define the structure and data types of the delta table to be populated
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.${table} (
# MAGIC order_id STRING,
# MAGIC order_date DATE,
# MAGIC order_time STRING,
# MAGIC order_status STRING,
# MAGIC sales_channel STRING,
# MAGIC customer_id STRING,
# MAGIC customer_name STRING,
# MAGIC customer_age INT,
# MAGIC gender STRING,
# MAGIC customer_segment STRING,
# MAGIC customer_type STRING,
# MAGIC customer_city STRING,
# MAGIC customer_state STRING,
# MAGIC customer_country STRING,
# MAGIC region STRING,
# MAGIC customer_postal_code INT,
# MAGIC payment_method STRING,
# MAGIC payment_status STRING,
# MAGIC currency STRING,
# MAGIC shipping_method STRING,
# MAGIC warehouse STRING,
# MAGIC delivery_days DOUBLE,
# MAGIC estimated_delivery_days DOUBLE,
# MAGIC delivery_status STRING,
# MAGIC return_status STRING,
# MAGIC return_reason STRING,
# MAGIC customer_rating DOUBLE,
# MAGIC review_sentiment STRING,
# MAGIC customer_review STRING,
# MAGIC marketing_channel STRING,
# MAGIC campaign_name STRING,
# MAGIC coupon_code STRING,
# MAGIC loyalty_points_earned INT,
# MAGIC loyalty_points_redeemed INT,
# MAGIC quantity INT,
# MAGIC gross_sales DOUBLE,
# MAGIC discount_amount DOUBLE,
# MAGIC tax_amount DOUBLE,
# MAGIC shipping_cost DOUBLE,
# MAGIC net_sales DOUBLE,
# MAGIC product_cost DOUBLE,
# MAGIC profit DOUBLE,
# MAGIC profit_margin_percentage DOUBLE,
# MAGIC customer_lifetime_value DOUBLE,
# MAGIC is_repeat_customer BOOLEAN,
# MAGIC customer_order_count INT,
# MAGIC ingestion_time TIMESTAMP,
# MAGIC file_modification_time TIMESTAMP,
# MAGIC source_file STRING,
# MAGIC file_path STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://${schema}@${storageName}.dfs.core.windows.net/${table}"

# COMMAND ----------

# Define the function to get AutoLoader batch metadata and add it to the temp view dataframe which will upsert the delta table

def process_batch(batch_df, batch_id):
    batch_final = (batch_df.withColumn('ingestion_time', current_timestamp())\
                            .withColumn('file_modification_time', col('_metadata.file_modification_time'))\
                            .withColumn('source_file', col('_metadata.file_name'))\
                            .withColumn('file_path', col('_metadata.file_path')))
    
    
    # Initialize the list of data quality checks
    dq_rows = []

    # Insert duplicate records in the data quality checks
    dups = (batch_final.groupBy(*key_cols)
                        .agg(count("*").alias("occurrences"),
                             first("source_file").alias("source_file"),
                             first("file_path").alias("file_path"))
                        .filter(col("occurrences") > 1)
                        .select(lit("duplicate").alias("check_type"),
                                concat_ws(" | ", *key_cols).alias("key_value"),
                                col("occurrences").cast("string").alias("detail"),
                                lit(f"{catalog}.{schema}.{table}").alias("source_table"),
                                "source_file", "file_path"))
    dq_rows.append(dups)

    # Insert rescued records in the data quality checks
    rescued = (batch_final.filter(col("_rescued_data").isNotNull())
                           .select(lit("rescued_data").alias("check_type"),
                                   concat_ws(" | ", *key_cols).alias("key_value"),
                                   col("_rescued_data").alias("detail"),
                                   lit(f"{catalog}.{schema}.{table}").alias("source_table"),
                                   "source_file", "file_path"))
    dq_rows.append(rescued)

    # Insert duplicate records in the data quality checks
    null_condition = reduce(lambda a, b: a | b, [col(c).isNull() for c in key_cols])
    null_detail = concat_ws(",", *[when(col(c).isNull(), lit(c)) for c in key_cols])

    null_keys = (batch_final.filter(null_condition)
                             .select(lit("null_key").alias("check_type"),
                                     concat_ws(" | ", *key_cols).alias("key_value"),
                                     null_detail.alias("detail"),
                                     lit(f"{catalog}.{schema}.{table}").alias("source_table"),
                                     "source_file", "file_path"))
    dq_rows.append(null_keys)

    # Definition of data quality checks as a dataframe appending each row of the list
    dq_log = dq_rows[0]
    for df_dq in dq_rows[1:]:
        dq_log = dq_log.unionByName(df_dq)
    dq_log = dq_log.withColumn("detected_at", current_timestamp())

    # Write the data quality checks to the table
    if dq_log.limit(1).count() > 0:
        dq_log.write.mode("append").saveAsTable(f"{catalog}.{schema}.dq_issues_log")

    # Clean the batch of duplicates and nulls
    batch_final = (batch_final
        .dropDuplicates(key_cols)
        .filter(~null_condition))
    
    # Write the batch to the table via a temp view
    batch_final.createOrReplaceTempView("df_temp_view")

    merge_condition = " AND ".join([f"TARGET.{c} = SOURCE.{c}" for c in key_cols])

    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.{table} AS TARGET
        USING df_temp_view AS SOURCE
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
    """)

# COMMAND ----------

# Call the function on the batches generated with the readStream

df.writeStream\
   .foreachBatch(process_batch)\
   .option("checkpointLocation", checkpoint_path)\
   .trigger(availableNow=True)\
   .start()\
   .awaitTermination()