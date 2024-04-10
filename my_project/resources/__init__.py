from dagster import EnvVar


from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster_snowflake import SnowflakeResource
from dagster_snowflake_pandas import SnowflakePandasIOManager
from pathlib import Path

import os

dbt_project_directory = os.getenv("DBT_PROJECT_DIRECTORY")
environment = os.getenv("ENVIRONMENT")


dbt_path = Path(dbt_project_directory).absolute()

dbt = DbtCliResource(
    project_dir=dbt_path,
    profiles_dir=dbt_path
)

resources = {
    "database": DuckDBResource(database=os.getenv("DUCKDB_DATABASE_LOCATION")),
    "io_manager": DuckDBPandasIOManager(database=os.getenv("DUCKDB_DATABASE_LOCATION")),
    "dbt": dbt
}