-- Databricks notebook source
-- MAGIC %md # Functions notebook part 1
-- MAGIC
-- MAGIC #### Purpose of this notebook
-- MAGIC This notebook defines the SQL functions necessary to execute the subsequent query exemplar / performance testing notebooks.

-- COMMAND ----------

select current_schema()

-- COMMAND ----------

-- MAGIC %md ### `string_array_as_double(string_array ARRAY < STRING >)`
-- MAGIC
-- MAGIC Takes in input string array `string_array` and returns an array of the same length with all elements cast from `StringType` to `DoubleType`. This is used when extracting coordinates from a WKT geometry.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION string_array_as_double (string_array ARRAY < STRING >)
RETURNS ARRAY < DOUBLE >
COMMENT 'Cast an array of string types to an array of double types'
RETURN transform(string_array, i -> double(i))

-- COMMAND ----------

-- MAGIC %md ### `extract_coords(geom STRING)`
-- MAGIC
-- MAGIC Takes a WKT geometry `geom` and uses regular expressions to return two arrays of `x`s and `y`s.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION extract_coords (geom STRING)
RETURNS MAP < STRING, ARRAY < DOUBLE > >
COMMENT "Extract all x and y coordinates from `geom`, a well-known-text geometry"
RETURN MAP(
  "x",
  string_array_as_double(
    regexp_extract_all(geom, "(-?\\d+.?\\d+)\\s")
  ),
  "y",
  string_array_as_double(
    regexp_extract_all(geom, "\\s(-?\\d+.?\\d+)[,)]")
  )
)

-- COMMAND ----------

-- MAGIC %md ### `envelope(geom STRING)`
-- MAGIC
-- MAGIC Takes a WKT geometry `geom`, computes the min and max x and y locations and returns these as a `MapType`.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION envelope (geom STRING)
RETURNS MAP < STRING, DOUBLE >
COMMENT "Compute the envelope around `geom`, a well-known-text geometry"
RETURN
  WITH coords AS (SELECT extract_coords(geom) AS coord_array)
  SELECT MAP(
    "xmin", array_min(coord_array["x"]), 
    "ymin", array_min(coord_array["y"]), 
    "xmax", array_max(coord_array["x"]), 
    "ymax", array_max(coord_array["y"])) 
FROM coords

-- COMMAND ----------

-- MAGIC %md ### `great_circle_distance(x1 DOUBLE, y1 DOUBLE, x2 DOUBLE, y2 DOUBLE)`
-- MAGIC
-- MAGIC Takes a pair of x and y coordinates and returns the [great-circle distance](https://en.wikipedia.org/wiki/Great-circle_distance) in metres between these points.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION great_circle_distance (
  x1 DOUBLE,
  y1 DOUBLE,
  x2 DOUBLE,
  y2 DOUBLE
)
RETURNS DOUBLE
COMMENT "Returns the distance (in metres) between two points."
RETURN 6378137 * acos(
  sin(radians(y1)) * sin(radians(y2)) + 
  cos(radians(y1)) * cos(radians(y2)) * cos(radians(x2) - radians(x1))
)
