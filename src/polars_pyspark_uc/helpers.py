import os
import polars as pl
from deltalake import DeltaTable
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

def set_env_vars():
    ws = WorkspaceClient()
    load_dotenv() # gets DATABRICKS_ACCESS_TOKEN from .env file
    os.environ["DATABRICKS_WORKSPACE_URL"] = ws.config.host


def polars_read_uc(table_path: str) -> pl.DataFrame:
    table_fqdn = f"uc://{table_path}"
    dt = DeltaTable(table_fqdn)
    # You can also filter things here before pulling it in memory
    print(dt.table_uri)
    return pl.from_arrow(dt.to_pyarrow_table())
