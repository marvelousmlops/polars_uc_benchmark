# Databricks notebook source
# Run this code on Databricks!
%pip install -r ../requirements.txt

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

from polars_pyspark_uc.config import ProjectConfig
from polars_pyspark_uc.init_benchmark import create_table_no_deletion_vectors

# COMMAND ----------

config = ProjectConfig.from_yaml(config_path="../project_config.yml")
CATALOG = config.catalog
SCHEMA_BASE = config.schema
VOLUME_NAME = config.volume
TABLES_PATH = "data/tables"
url = config.url

spark = DatabricksSession.builder.getOrCreate()
spark.catalog.setCurrentCatalog(CATALOG)

# COMMAND ----------

# Download folder data to volumes, structure:
# data
# --> tables
#      --> scale-1.0
#      --> scale-3.0
#      --> scale-10.0
#      --> scale-25.0
url = config.url
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BASE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BASE}.{VOLUME_NAME}")
gdown.download_folder(url, quiet=False, output=f"/Volumes/{CATALOG}/{SCHEMA_BASE}/{VOLUME_NAME}")

# COMMAND ----------

from tqdm.auto import tqdm
from joblib import Parallel, delayed

delayed_create_table_no_deletion_vector = delayed(create_table_no_deletion_vectors)

scales = [1, 3, 10, 25]
for scale in tqdm(scales, leave=False, desc="Scales: "):
    schema = f"{SCHEMA_BASE}_scale_{scale}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

    SCALE_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BASE}/raw/tables/scale-{scale}.0"

    tables = os.listdir(SCALE_PATH)
    # NOTE: Make sure you grant yourself the `EXTERNAL USE SCHEMA` privilege on the schemas!
    Parallel(n_jobs=len(tables), backend="threading")(
        delayed_create_table_no_deletion_vector(
            spark=spark,
            scale_path=SCALE_PATH,
            catalog=CATALOG,
            schema=schema,
            table_fn=table_fn,
            scale=scale,
        )
        for table_fn in tables
    )
