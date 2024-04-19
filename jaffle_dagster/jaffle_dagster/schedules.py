"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition
# from dagster_dbt import build_schedule_from_dbt_selection

from .assets import jaffle_shop_dbt_asset_job

schedules = [
     ScheduleDefinition(
         name='data_model_materialization',
         job=jaffle_shop_dbt_asset_job,
         cron_schedule="0 0 * * *",
        #  dbt_select="",
     ),
]