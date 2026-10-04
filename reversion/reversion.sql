-- Databricks notebook source
-- Tables
DROP TABLE IF EXISTS production.gold.ecommerce_sales_summary;
DROP TABLE IF EXISTS production.silver.ecommerce_sales_transformed;
DROP TABLE IF EXISTS production.silver.dq_issues_log;
DROP TABLE IF EXISTS production.bronze.ecommerce_sales;
DROP TABLE IF EXISTS production.bronze.customer_master;
DROP TABLE IF EXISTS production.bronze.order_items;
DROP TABLE IF EXISTS production.bronze.product_catalog;
DROP TABLE IF EXISTS production.bronze.dq_issues_log;

DROP TABLE IF EXISTS development.gold.ecommerce_sales_summary;
DROP TABLE IF EXISTS development.silver.ecommerce_sales_transformed;
DROP TABLE IF EXISTS development.silver.dq_issues_log;
DROP TABLE IF EXISTS development.bronze.ecommerce_sales;
DROP TABLE IF EXISTS development.bronze.customer_master;
DROP TABLE IF EXISTS development.bronze.order_items;
DROP TABLE IF EXISTS development.bronze.product_catalog;
DROP TABLE IF EXISTS development.bronze.dq_issues_log;

-- Delta Share
DROP SHARE IF EXISTS sales_share;
DROP RECIPIENT IF EXISTS powerbi_recipient;

-- Catalogs (cascades schemas)
DROP CATALOG IF EXISTS production CASCADE;
DROP CATALOG IF EXISTS development CASCADE;

-- External Locations
DROP EXTERNAL LOCATION IF EXISTS ext_loc_production_metastore;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_production_raw;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_production_bronze;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_production_silver;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_production_gold;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_production_checkpoints;

DROP EXTERNAL LOCATION IF EXISTS ext_loc_development_metastore;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_development_raw;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_development_bronze;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_development_silver;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_development_gold;
DROP EXTERNAL LOCATION IF EXISTS ext_loc_development_checkpoints;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for storageName in ["adlsjscgdevelopment", "adlsjscgproduction"]:
-- MAGIC     for container in ["bronze", "silver", "gold", "metastore"]:
-- MAGIC         dbutils.fs.rm(f"abfss://{container}@{storageName}.dfs.core.windows.net/", True)