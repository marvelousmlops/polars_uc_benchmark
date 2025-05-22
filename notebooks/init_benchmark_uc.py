# Databricks notebook source
# run on Databricks
# MAGIC %pip install -r requirements.txt

# COMMAND ----------
# MAGIC %restart_python

# COMMAND ----------
from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent / 'src'))
# COMMAND ----------

import os
import gdown
from databricks.connect import DatabricksSession
from joblib import Parallel, delayed

from polars_pyspark_uc.config import ProjectConfig
from polars_pyspark_uc.init_benchmark import create_table_no_deletion_vectors


# COMMAND ----------
config = ProjectConfig.from_yaml(config_path="../project_config.yml")
CATALOG = config.catalog
SCHEMA_BASE = config.schema
VOLUME_NAME = config.volume
TABLES_PATH = "../data/tables"
url = config.url

spark = DatabricksSession.builder.getOrCreate()
spark.catalog.setCurrentCatalog(CATALOG)

# COMMAND ----------
# Download folder data, structure:
# data
# --> tables
#      --> scale-1.0
#      --> scale-3.0
#      --> scale-10.0
#      --> scale-25.0
url = config.url
gdown.download_folder(url, quiet=False)

# COMMAND ----------
delayed_create_table = delayed(create_table_no_deletion_vectors)

scales = [1, 3] #, 10, 25]
for scale in scales:
    schema = f"{SCHEMA_BASE}_{scale}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

    SCALE_PATH = os.path.join(TABLES_PATH, f"scale-{scale}.0")
    tables = os.listdir(SCALE_PATH)
    Parallel(backend="threading", n_jobs=len(tables))(
        delayed_create_table(spark, SCALE_PATH, CATALOG, schema, table_fn)
        for table_fn in tables
    )
