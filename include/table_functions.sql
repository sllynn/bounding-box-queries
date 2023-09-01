-- Databricks notebook source
-- MAGIC %md # Functions notebook part 2
-- MAGIC
-- MAGIC #### Purpose of this notebook
-- MAGIC This notebook defines additional SQL functions that are dependent on additional data being available in the `geo_perf` schema.

-- COMMAND ----------

-- MAGIC %md ### `best_h3(geom STRING)`
-- MAGIC
-- MAGIC Estimtes an optimal H3 grid index resolution for use in querying a points dataset based on the size of the query bounding box. Returns an integer between 0 and 15 inclusive. The method used here is to choose the resolution that results that results in at least 100 index cells lying along the diagonal bisecting the bounding box. This will typically generate ~4,000 indexes on which to perform the final join.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION best_h3 (geom STRING)
RETURNS LONG
COMMENT 'Attempts to determine the best h3 index resolution for clipping by the input `geom`.'
RETURN
  WITH qc AS (
    SELECT
      envelope(geom) AS bbox
  ),
  dist AS (
    SELECT
      great_circle_distance(
        qc.bbox.xmin,
        qc.bbox.ymin,
        qc.bbox.xmax,
        qc.bbox.ymax
      ) AS diagonal_distance
    FROM
      qc
  )
  SELECT
    first(res)
  FROM
    h3_resolutions,
    dist
  WHERE
    dist.diagonal_distance / (100 * 2) BETWEEN edge_length_next AND edge_length

-- COMMAND ----------

-- MAGIC %md ### `test_geom()`
-- MAGIC
-- MAGIC Takes a WKT geometry `geom` and uses regular expressions to return two arrays of `x`s and `y`s.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION test_geom (location_id BIGINT)
RETURNS STRING
COMMENT 'Pick a test geometry from the set based on a given `location_id`'
RETURN
  SELECT
    first(g.bbox)
  FROM
    test_geometries g
  WHERE
    CASE WHEN location_id = 0
      THEN g.LocationID = ceil(rand() * (SELECT count(*) FROM test_geometries))
      ELSE g.LocationID = location_id
    END
