import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import jaffle_shop_dbt_assets, raw_customers, order_count_chart, jaffle_shop_dbt_asset_job
from .constants import dbt_project_dir
from .schedules import schedules

from .slack import slack_on_run_failure

defs = Definitions(
    assets=[raw_customers, jaffle_shop_dbt_assets, order_count_chart],
    jobs=[jaffle_shop_dbt_asset_job],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    sensors=[slack_on_run_failure],
)

