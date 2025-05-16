import os
from joblib import Parallel, delayed
from polars_pyspark_uc.init_benchmark import upload_file_into_volume, create_table_no_deletion_vectors, upload_and_create_table 
from polars_pyspark_uc.config import ProjectConfig
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

config = ProjectConfig.from_yaml(config_path="../project_config.yml")
CATALOG = config.catalog
SCHEMA_BASE = config.schema
VOLUME_NAME = config.volume
TABLES_PATH = "../data/tables"

delayed_upload_and_create = delayed(upload_and_create_table)
spark = DatabricksSession.builder.getOrCreate()

spark.catalog.setCurrentCatalog(CATALOG)
scales = [1, 3, 10, 25]
for scale in scales:
    schema = f"{SCHEMA_BASE}_{scale}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{schema}.{VOLUME_NAME}")

    SCALE_PATH = os.path.join(TABLES_PATH, f"scale-{scale}.0")
    tables = os.listdir(SCALE_PATH)
    Parallel(backend="threading", n_jobs=len(tables))(
        delayed_upload_and_create(SCALE_PATH, CATALOG, schema, VOLUME_NAME, table_fn)
        for table_fn in tables
    )
