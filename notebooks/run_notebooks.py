# Databricks notebook source
dbutils.notebook.run("run_polars_against_uc", {"scale": 1})

# COMMAND ----------
dbutils.notebook.run("run_polars_against_uc", {"scale": 3})

# COMMAND ----------
dbutils.notebook.run("run_spark_sql_agains_uc", {"scale": 1})

# COMMAND ----------
dbutils.notebook.run("run_spark_sql_agains_uc", {"scale": 3})
