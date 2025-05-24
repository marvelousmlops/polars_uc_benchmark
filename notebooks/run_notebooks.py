# Databricks notebook source
dbutils.notebook.run(path="run_polars_against_uc", timeout_seconds=1000, arguments={"scale": "1"})

# COMMAND ----------

dbutils.notebook.run(path="run_polars_against_uc", timeout_seconds=1000, arguments={"scale": "3"})

# COMMAND ----------

dbutils.notebook.run(path="run_spark_sql_against_uc", timeout_seconds=1000, arguments={"scale": "1"})

# COMMAND ----------

dbutils.notebook.run(path="run_spark_sql_against_uc", timeout_seconds=1000, arguments={"scale": "3"})
