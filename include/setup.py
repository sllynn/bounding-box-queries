# Databricks notebook source
# MAGIC %md # Setup notebook
# MAGIC
# MAGIC #### Purpose of this notebook
# MAGIC This notebook creates the data objects required to run the subsequent query exemplar / performance testing notebooks.

# COMMAND ----------

user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
user_name = user_email.split("@")[0].replace(".", "_")
schema_name = "geo_perf"

# COMMAND ----------

spark.sql(f"create catalog if not exists {user_name}")
spark.sql(f"use catalog {user_name}")

# COMMAND ----------

if dbutils.widgets.get("rebuildAll") == "True":
  print("Rebuilding datasets")
  spark.sql(f"drop schema {schema_name} cascade")

# COMMAND ----------

spark.sql(f"create schema if not exists {schema_name}")
spark.sql(f"use {schema_name}")
