-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- Parameters preparation

CREATE WIDGET TEXT catalog DEFAULT "development";
CREATE WIDGET TEXT externalLocationName DEFAULT "ext_loc_development";

-- COMMAND ----------

-- Permissions for data analysts, read only

GRANT USE CATALOG ON CATALOG ${catalog} TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.bronze TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.silver TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `data-analysts`;

-- COMMAND ----------

-- Permissions for data engineers in schemas and in external locations

GRANT USE CATALOG ON CATALOG ${catalog} TO `data-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA ${catalog}.bronze TO `data-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA ${catalog}.silver TO `data-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA ${catalog}.gold TO `data-engineers`;

GRANT READ FILES ON EXTERNAL LOCATION ${externalLocationName}_raw TO `data-engineers`;
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ${externalLocationName}_bronze TO `data-engineers`;
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ${externalLocationName}_silver TO `data-engineers`;
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ${externalLocationName}_gold TO `data-engineers`;