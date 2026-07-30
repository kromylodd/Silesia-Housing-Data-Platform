import logging
import os

import requests

logger = logging.getLogger(__name__)

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def notify_discord_on_failure(context):
    """DAG-level on_failure_callback.

    Airflow 2.6+ fires this once when a DagRun's state resolves to `failed`
    (i.e. after retries on the failing task(s) are exhausted) - NOT once per
    failed task instance. With ~33 tasks in this DAG (8 cities x 4 tasks +
    dbt_build), a per-task callback would spam the channel; DAG-level keeps
    it to one message per bad run.
    """
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL not set, skipping Discord alert")
        return

    dag_run = context.get("dag_run")
    dag_id = context["dag"].dag_id
    logical_date = context.get("logical_date") or context.get("execution_date")
    run_id = dag_run.run_id if dag_run else "unknown"

    failed_tasks = []
    if dag_run:
        failed_tasks = [
            ti.task_id for ti in dag_run.get_task_instances() if ti.state == "failed"
        ]

    message = (
        f"**Housing pipeline failed**\n"
        f"DAG: `{dag_id}`\n"
        f"Run: `{run_id}`\n"
        f"Logical date: `{logical_date}`\n"
        f"Failed tasks: {', '.join(failed_tasks) if failed_tasks else 'unknown'}"
    )

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"Failed to send Discord alert: {exc}")