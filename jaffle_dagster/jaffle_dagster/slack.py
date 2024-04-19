
import os
from dagster import RunFailureSensorContext
from dagster_slack import make_slack_on_run_failure_sensor


def my_message_fn(context: RunFailureSensorContext) -> str:
    return (
        f"Job {context.dagster_run.job_name} failed!"
        f"Error: {context.failure_event.message}"
    )

slack_on_run_failure = make_slack_on_run_failure_sensor(
    channel="C048ZPSJE4E",
    slack_token=os.getenv("MY_SLACK_TOKEN"),
    text_fn=my_message_fn,
    webserver_base_url="http://127.0.0.1:3000",
)


