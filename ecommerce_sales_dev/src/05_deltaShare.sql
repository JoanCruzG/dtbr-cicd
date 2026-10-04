-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- Parameters preparation

CREATE WIDGET TEXT catalog DEFAULT "production";

-- COMMAND ----------

-- Creation of the recipient for Delta Share

CREATE RECIPIENT IF NOT EXISTS powerbi_recipient;

-- COMMAND ----------

-- Creation and setup of the share

CREATE SHARE IF NOT EXISTS sales_share
  COMMENT 'Gold layer for the Power BI dashboard';

ALTER SHARE sales_share ADD TABLE ${catalog}.gold.ecommerce_sales_summary;

GRANT SELECT ON SHARE sales_share TO RECIPIENT powerbi_recipient;