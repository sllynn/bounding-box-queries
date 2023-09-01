# Databricks notebook source
# MAGIC %md # Bounding boxes
# MAGIC ### Purpose of this notebook
# MAGIC This notebook will read the taxi pickup zones files (where neighbourhood extents are expressed in WKT) which we can use to generate a realistic set of bounding boxes to test our query strategies.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog}; USE SCHEMA ${schema};

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
folder_path = "/".join(notebook_path.split("/")[:-1])
local_folder_path = f"/Workspace{folder_path}"
local_file_path = f"{local_folder_path}/taxi_zones.csv"
local_file_path

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, lit, concat, expr
from pyspark.sql import Column

def polyFromEnvelope(envelope: "ColumnOrName") -> Column:
    """
    Translate a set of min / max x and y locations 
    representing the envelope of a polygon into a Well-known Text (WKT) 
    representation of its bounding rectangle.

    Parameters
    ----------
    envelope : Column (MapType)
        A map providing `xmin`, `ymin`, `xmax` and `ymax` values.

    Returns
    -------
    Column (StringType)
        A WKT geometry

    """
    envelope_col = envelope if isinstance(envelope, Column) else col(envelope)
    xmin, ymin, xmax, ymax = (
        envelope_col.getItem("xmin"),
        envelope_col.getItem("ymin"),
        envelope_col.getItem("xmax"),
        envelope_col.getItem("ymax"),
    )
    poly_coords = [
      [xmin, ymin],
      [xmin, ymax],
      [xmax, ymax],
      [xmax, ymin],
      [xmin, ymin]
    ]
    poly_coords_wkt = [(x, lit(" "), y, lit(","), lit(" ")) for x, y in poly_coords]
    poly_wkt = [v for c in poly_coords_wkt for v in c][:-2]
    return concat(lit("POLYGON (("), *poly_wkt, lit("))"))

# COMMAND ----------

(
  spark.createDataFrame(pd.read_csv(local_file_path))
  .select("OBJECTID", "zone", "LocationID", "borough", "the_geom")
  .withColumn("bbox", polyFromEnvelope(expr("envelope(the_geom)")))
  .write
  .mode("overwrite")
  .saveAsTable("test_geometries")
  )
