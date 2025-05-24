import os
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession


def create_table_no_deletion_vectors(spark: SparkSession | DatabricksSession,
scale_path: str, catalog: str, schema: str, table_fn: str, scale: int):
    df = spark.read.parquet(os.path.join(scale_path, table_fn))
    table_name = table_fn.split(".")[0]
    table_path = f"{catalog}.{schema}.{table_name}"

    # === Create Random Chunks ===
    weights = [1.0] * scale  # equal weights
    chunks = df.randomSplit(weights, seed=42)

    # === Write Each Chunk to Delta Table ===
    for i, chunk in enumerate(chunks):
        #write_mode = "overwrite" if i == 0 else "append"
        chunk.write.mode("overwrite").saveAsTable(table_path)
    spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);")
    print(f"Created table: {table_path}")
