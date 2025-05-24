import polars as pl
from deltalake import DeltaTable


def polars_read_uc(table_path: str) -> pl.DataFrame:
    table_fqdn = f"uc://{table_path}"
    dt = DeltaTable(table_fqdn)
    # You can also filter things here before pulling it in memory
    print(dt.table_uri)
    return pl.from_arrow(dt.to_pyarrow_table())
