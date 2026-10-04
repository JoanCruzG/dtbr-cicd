-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

CREATE WIDGET TEXT storageName DEFAULT "adlsjscgdevelopment";
CREATE WIDGET TEXT externalLocationName DEFAULT "ext_loc_development";
CREATE WIDGET TEXT catalog DEFAULT "development";
CREATE WIDGET TEXT credentialName DEFAULT "cred_adls_dev";


-- COMMAND ----------

-- External locations, one per storage account, each using its own credential

CREATE EXTERNAL LOCATION IF NOT EXISTS ${externalLocationName}_metastore URL 'abfss://metastore@${storageName}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ${credentialName});
CREATE EXTERNAL LOCATION IF NOT EXISTS ${externalLocationName}_raw URL 'abfss://raw@${storageName}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ${credentialName});
CREATE EXTERNAL LOCATION IF NOT EXISTS ${externalLocationName}_bronze URL 'abfss://bronze@${storageName}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ${credentialName});
CREATE EXTERNAL LOCATION IF NOT EXISTS ${externalLocationName}_silver URL 'abfss://silver@${storageName}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ${credentialName});
CREATE EXTERNAL LOCATION IF NOT EXISTS ${externalLocationName}_gold URL 'abfss://gold@${storageName}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ${credentialName});
CREATE EXTERNAL LOCATION IF NOT EXISTS ${externalLocationName}_checkpoints URL 'abfss://checkpoints@${storageName}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ${credentialName});

-- COMMAND ----------

LIST 'abfss://raw@${storageName}.dfs.core.windows.net/';
SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

-- Catalog, one for each environment

CREATE CATALOG IF NOT EXISTS ${catalog} MANAGED LOCATION 'abfss://metastore@${storageName}.dfs.core.windows.net/${catalog}';

-- COMMAND ----------

-- Schemas, one per environment and layer

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze;
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver;
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold;

-- COMMAND ----------

-- Data quality issues log table definition, applies for bronze ingestion only

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.dq_issues_log (
  check_type STRING,
  key_value STRING,
  detail STRING,
  source_table STRING,
  source_file STRING,
  file_path STRING,
  detected_at TIMESTAMP
)
USING DELTA
LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/dq_issues_log"

-- COMMAND ----------

-- Data quality issues log table definition, applies for silver transformation only

CREATE TABLE IF NOT EXISTS ${catalog}.silver.dq_issues_log (
  check_type STRING,
  key_value STRING,
  detail STRING,
  source_table STRING,
  source_file STRING,
  file_path STRING,
  detected_at TIMESTAMP
)
USING DELTA
LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/dq_issues_log"

-- COMMAND ----------

DESCRIBE CATALOG EXTENDED development;