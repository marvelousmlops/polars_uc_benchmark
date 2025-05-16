import os
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession

def upload_file_into_volume(
    scale_path: str, catalog: str, schema: str, volume_name: str, table_fn: str
):
    table_fnp = os.path.join(scale_path, table_fn)
    volume_file_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{table_fn}"
    with open(table_fnp, "rb") as f:
        ws.files.upload(volume_file_path, f)


def create_table_no_deletion_vectors(
    spark: SparkSession | DatabricksSession, catalog: str, schema: str, volume_name: str, table_fn: str
):
    table_name = table_fn.split(".")[0]
    volume_file_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{table_fn}"
    table_path = f"{catalog}.{schema}.{table_name}"

    storage_account_name = "tfucstorage"
    storage_account_container_name = "unity-catalog-ext"
    cloud_uri = f"abfss://{storage_account_container_name}@{storage_account_name}.dfs.core.windows.net/{schema}/{table_name}"
    options = {"path": cloud_uri, "delta.enableDeletionVectors": "false"}

    df = spark.read.parquet(volume_file_path)
    df.write.format("delta").options(**options).mode("overwrite").saveAsTable(
        table_path
    )


def upload_and_create_table(scale_path, catalog, schema, volume_name, table_fn):
    upload_file_into_volume(scale_path, catalog, schema, volume_name, table_fn)
    create_table_no_deletion_vectors(catalog, schema, volume_name, table_fn)
    print(f"Created table: {catalog}.{schema}.{table_fn}")
