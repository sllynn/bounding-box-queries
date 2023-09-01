-- Databricks notebook source
-- MAGIC %md ## Strategy 2: query using H3 indexes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1: Index the points collection at every resolution
-- MAGIC
-- MAGIC In order to perform these joins efficiently on the fly, we need to index our points data at multiple resolutions. In the example here, I've chosen to index the points at all 16 H3 resolution levels.

-- COMMAND ----------

-- MAGIC %run "./include/setup" $rebuildAll=False $UC=True

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.databricks.sql.functions import h3_longlatash3
-- MAGIC from pyspark.sql.dataframe import DataFrame
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC
-- MAGIC def index_at_res(df: DataFrame, lon: "ColumnOrName", lat: "ColumnOrName", res: int) -> DataFrame:
-- MAGIC     cols = df.columns
-- MAGIC     new_col = f"h3_{res}"
-- MAGIC     return df.withColumn(new_col, h3_longlatash3(lon, lat, res)).select(new_col, *cols)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC trips_df = spark.table("nyctaxi_yellow")
-- MAGIC for i in range(16):
-- MAGIC   trips_df = trips_df.transform(index_at_res, "pickup_longitude", "pickup_latitude", i)
-- MAGIC
-- MAGIC display(trips_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC   trips_df
-- MAGIC   .write
-- MAGIC   .mode("overwrite")
-- MAGIC   .saveAsTable("nyctaxi_yellow_idxd")
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %md ### Step 2: Execute joins that depend on the size of the input bounding box
-- MAGIC
-- MAGIC When it comes to query execution time, we select the resolution to use in the join based on the size of the incoming bounding box by using the function `best_h3()`.
-- MAGIC
-- MAGIC #### Example

-- COMMAND ----------

SELECT best_h3("POLYGON ((-73.983 40.718,-73.983 40.729,-73.971 40.729,-73.971 40.718,-73.983 40.718))")

-- COMMAND ----------

OPTIMIZE nyctaxi_yellow_idxd 
ZORDER BY (h3_12)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION filter_trips_h3 (geom STRING)
RETURNS TABLE
RETURN
  WITH opt_idx AS (
    SELECT best_h3(geom) AS res
  ), qi AS (
    SELECT explode(h3_coverash3(geom, res)) AS idx FROM opt_idx
  ), trips AS (
    SELECT
      p.*,
      CASE o.res
        WHEN 0 THEN p.h3_0
        WHEN 1 THEN p.h3_1
        WHEN 2 THEN p.h3_2
        WHEN 3 THEN p.h3_3
        WHEN 4 THEN p.h3_4
        WHEN 5 THEN p.h3_5
        WHEN 6 THEN p.h3_6
        WHEN 7 THEN p.h3_7
        WHEN 8 THEN p.h3_8
        WHEN 9 THEN p.h3_9
        WHEN 10 THEN p.h3_10
        WHEN 11 THEN p.h3_11
        WHEN 12 THEN p.h3_12
        WHEN 13 THEN p.h3_13
        WHEN 14 THEN p.h3_14
      END AS join_idx
    FROM
    nyctaxi_yellow_idxd p
    CROSS JOIN opt_idx o
  )
  SELECT
    trips.*
  FROM
    trips INNER JOIN qi 
      ON trips.join_idx = qi.idx

-- COMMAND ----------

-- MAGIC %md ### Query examples
-- MAGIC
-- MAGIC As before, run these examples on your SQL warehouse for a more accurate estimate of performance.

-- COMMAND ----------

-- Important:
-- update the first of these statements with the name of your catalog

USE CATALOG firstname_lastname;
USE SCHEMA geo_perf;

-- COMMAND ----------

CREATE OR REPLACE TABLE test_results_h3 AS
SELECT 
  *
FROM 
  filter_trips_h3("POLYGON ((-73.983 40.718,-73.983 40.729,-73.971 40.729,-73.971 40.718,-73.983 40.718))")

-- COMMAND ----------

CREATE OR REPLACE TABLE test_results_h3 AS
SELECT 
  *
FROM 
  filter_trips_h3("POLYGON ((-74.083 40.618,-74.083 40.829,-73.871 40.829,-73.871 40.618,-74.083 40.618))")

-- COMMAND ----------

CREATE OR REPLACE TABLE test_results_h3 AS
SELECT 
  *
FROM 
  filter_trips_h3("POLYGON ((-75.083 39.618,-75.083 40.829,-73.871 40.829,-73.871 39.618,-75.083 39.618))")

-- COMMAND ----------


