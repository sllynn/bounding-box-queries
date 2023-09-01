# Databricks notebook source
# MAGIC %md ## Initial setup
# MAGIC
# MAGIC ### Purpose of this notebook
# MAGIC Run this notebook on a standard cluster running DBR 13.3 LTS in 'Shared' or 'Assigned' access modes to build the required datasets and register the SQL functions needed to run the query examples.
# MAGIC - Datasets and functions will be registered in a catalog named `firstName_lastName` in the `geo_perf` schema.
# MAGIC - If this catalog does not already exist, the script will attempt to create it. For this to work correctly you will need [the `CREATE CATALOG` and `CREATE SCHEMA` privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html) on the metastore associated with your workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Note:
# MAGIC Switch the `$rebuildAll` to `True` to rebuild all of the datasets and re-register the SQL functions.

# COMMAND ----------

# MAGIC %run ./include/setup $rebuildAll=True $UC=True

# COMMAND ----------

# MAGIC %run ./include/scalar_functions

# COMMAND ----------

global_params = dict(
  catalog=spark.sql("select current_catalog() as c").first().c,
  schema=spark.sql("select current_schema() as s").first().s
  )

existing_tables = [r.tableName for r in spark.sql("show tables").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Note:
# MAGIC If you wish to rebuild one or more of the datasets, comment out the relevant `if` statements below.

# COMMAND ----------

if not "nyctaxi_yellow" in existing_tables:
  dbutils.notebook.run("./data/01 - Taxi data", 6000, global_params)
if not "test_geometries" in existing_tables:
  dbutils.notebook.run("./data/02 - Bounding boxes", 600, global_params)
if not "h3_resolutions" in existing_tables:
  dbutils.notebook.run("./data/03 - H3 reference data", 600, global_params)

# COMMAND ----------

# MAGIC %run "./include/table_functions"

# COMMAND ----------


