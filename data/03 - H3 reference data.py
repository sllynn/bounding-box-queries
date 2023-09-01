# Databricks notebook source
# MAGIC %md
# MAGIC ## H3 reference data notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog}; USE SCHEMA ${schema};

# COMMAND ----------

# MAGIC %pip install 'h3==4.0.0b2'

# COMMAND ----------

import h3
from pyspark.sql.functions import *
from pyspark.sql.window import Window

resolutions = [
  dict(
    res=r,
    num_cells=h3.get_num_cells(r),
    cell_area=h3.average_hexagon_area(r, "m^2"),
    edge_length=h3.average_hexagon_edge_length(r, "m")
    ) for r in range(16)]
resolutions = (
  spark.createDataFrame(resolutions)
  .withColumn("edge_length_next", lead("edge_length", 1).over(Window.orderBy("res")))
  .fillna(0)
  )
display(resolutions)

# COMMAND ----------

resolutions.write.mode("overwrite").saveAsTable("h3_resolutions")
