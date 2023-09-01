-- Databricks notebook source
-- MAGIC %md # Taxi data
-- MAGIC ### Purpose of this notebook
-- MAGIC This notebook makes a copy of the `nyctaxi` dataset for the query performance testing. A subsample of 50M trips is also generated in the `nyctaxi_yellow_50m` table.

-- COMMAND ----------

USE CATALOG ${catalog}; USE SCHEMA ${schema};

-- COMMAND ----------

CREATE OR REPLACE TABLE nyctaxi_yellow AS
SELECT
  *
FROM
  delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`

-- COMMAND ----------

OPTIMIZE nyctaxi_yellow 
ZORDER BY (pickup_latitude, pickup_longitude)

-- COMMAND ----------

CREATE OR REPLACE TABLE nyctaxi_yellow_50m AS
SELECT
  *
FROM
  delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
TABLESAMPLE (50000000 ROWS)

-- COMMAND ----------

OPTIMIZE nyctaxi_yellow_50m 
ZORDER BY (pickup_latitude, pickup_longitude)
