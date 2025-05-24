import os

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def create_table_no_deletion_vectors(
    spark: SparkSession | DatabricksSession, scale_path: str, catalog: str, schema: str, table_fn: str, scale: int
):
    df = spark.read.parquet(os.path.join(scale_path, table_fn))
    table_name = table_fn.split(".")[0]
    table_path = f"{catalog}.{schema}.{table_name}"

    df.write.mode("overwrite").saveAsTable(table_path)
    spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);")
    print(f"Created table: {table_path}")
