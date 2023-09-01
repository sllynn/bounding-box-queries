-- Databricks notebook source
-- MAGIC %md ## Strategy 1: query by bounding box limits
-- MAGIC
-- MAGIC In this notebook, you'll see how we can query the points dataset based on the limits of the query geometry.
-- MAGIC - Run this first part of the notebook on your standard cluster and the second part ('Example Queries') on a SQL warehouse.

-- COMMAND ----------

-- MAGIC %run "./include/setup" $rebuildAll=False

-- COMMAND ----------

-- MAGIC %md ### Step 1: Data retrieval function
-- MAGIC #### `filter_trips(geom STRING)`
-- MAGIC This function filters the large trips table based on the min / max x and y coordinates of `geom`. As well as using this from within a SQL statement in the notebook, you can call it directly through the SQL execution API.
-- MAGIC
-- MAGIC Note: this can be adapted to use the smaller, 50M row dataset by substituting `FROM nyctaxi_yellow` with `FROM nyctaxi_yellow_50m`.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION filter_trips (geom STRING)
RETURNS TABLE
RETURN
  SELECT
    *
  FROM
    nyctaxi_yellow
  WHERE
    pickup_longitude BETWEEN envelope(geom)['xmin'] AND envelope(geom)['xmax']
    AND pickup_latitude BETWEEN envelope(geom)['ymin'] AND envelope(geom)['ymax']

-- COMMAND ----------

-- MAGIC %md ### Step 2: Example queries
-- MAGIC Below there are a number of queries on different sized bounding boxes. We could select from any of the geometries in the `test_geoms` table by using the `test_geom(location_id BIGINT)` function. The example here is using `LocationId=4`, 'Alphabet City' with a reduction in the coordinate precision to make this easier to read.
-- MAGIC
-- MAGIC - Run this on your SQL warehouse for an accurate indication of performance.
-- MAGIC - Detailed query profiling statistics are available in the 'Query History' section of the DBSQL workspace.

-- COMMAND ----------

-- Important:
-- update the first of these statements with the name of your catalog

USE CATALOG firstName_lastName;
USE SCHEMA geo_perf;

-- COMMAND ----------

CREATE OR REPLACE TABLE test_results AS
SELECT 
  *
FROM 
  filter_trips("POLYGON ((-73.983 40.718,-73.983 40.729,-73.971 40.729,-73.971 40.718,-73.983 40.718))")

-- COMMAND ----------

CREATE OR REPLACE TABLE test_results AS
SELECT 
  *
FROM 
  filter_trips("POLYGON ((-74.083 40.618,-74.083 40.829,-73.871 40.829,-73.871 40.618,-74.083 40.618))")

-- COMMAND ----------

CREATE OR REPLACE TABLE test_results AS
SELECT 
  *
FROM 
  filter_trips("POLYGON ((-75.083 39.618,-75.083 40.829,-73.871 40.829,-73.871 39.618,-75.083 39.618))")
