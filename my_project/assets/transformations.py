from dagster import OpExecutionContext, WeeklyPartitionsDefinition, MonthlyPartitionsDefinition, asset
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator, DagsterDbtTranslatorSettings

from pathlib import Path

import json

dbt_manifest_path = Path("./dbt_project/target/manifest.json")

# @dbt_assets(
#     manifest=dbt_manifest_path
# )
# def general_project(context: OpExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()


















###



translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_asset_checks=True,
    )
)

WEEKLY_SELECTOR = "weekly_pop_rollup"
MONTHLY_SELECTOR = "monthly_pop_rollup"
GENERAL_EXCLUSION = WEEKLY_SELECTOR + " " + MONTHLY_SELECTOR

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=translator,
    exclude=GENERAL_EXCLUSION
)
def general_project(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=translator,
    select=WEEKLY_SELECTOR,
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01")
)
def weekly_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window
    dbt_vars = {
        "min_date": start.isoformat(),
        "max_date": end.isoformat()
    }
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from dbt.cli(dbt_build_args, context=context).stream()

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=translator,
    select=MONTHLY_SELECTOR,
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01")
)
def monthly_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window
    dbt_vars = {
        "min_date": start.isoformat(),
        "max_date": end.isoformat()
    }
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from dbt.cli(dbt_build_args, context=context).stream()