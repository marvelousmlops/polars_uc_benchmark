# Polars Benchmark on Databricks

This repository hosts the logic as copied from the original benchmark repo, here: https://github.com/pola-rs/polars-benchmark/tree/main


## Getting the data
The steps followed were:
0) *NOTE* Ensure that the `.venv` is created in the `polars-benchmark` repo. There are commands in the `make` file that reference it there. For this benchmark it makes no difference.
1) Create the data locally by running the Makefile: ensure that you set the parameter `SCALE_FACTOR=10.0` or different to size up or down the amount of data. with 10x it results in about a 2gb of total data (in parquet), with the biggest file being 60M rows (lineitem).
2) run the command `make data/tables/` and let it create the files for the corresponding SCALE_FACTOR
3) copy the parquet files over manually into a Volume in Unity Catalog
4) Run the ETL script over the files in the volume to create the tables with the correct options enabled.


## Enabling Unity Catalog
To enable Polars to read the data directly from Unity Catalog without invoking Spark requires a few settings to be set. These are listed below:
- Toggle `External Data Access` via the Metastore Toggle in the Catalog Explorer.
- Grant the `EXTERNAL_USE_SCHEMA` Privilege on the Schema level (where you will be reading the data from). This will be exposed once the toggle above has switched. *NOTE* this seems currently not possible via either the API / SDK / Terraform. Only the GUI can achieve this.

## Reading the data
This can be read directly either locally or directly within Databricks.
