"""Airflow DAG for periodically cleaning old task logs."""
from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import Iterable, List, Tuple

import airflow
import jinja2
from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from log_cleanup import build_cleanup_bash_script


logger = logging.getLogger(__name__)


def _is_truthy(value: str) -> bool:
    """Return ``True`` when *value* represents a truthy string."""

    return str(value).strip().lower() in {"true", "1", "yes", "on"}


def _get_base_log_folder() -> str:
    """Resolve the Airflow base log directory from the configuration."""

    try:
        folder = conf.get("core", "BASE_LOG_FOLDER")
    except Exception:
        folder = conf.get("logging", "BASE_LOG_FOLDER")

    folder = folder.rstrip("/")
    if not folder or folder.strip() == "":
        raise ValueError(
            "BASE_LOG_FOLDER variable is empty in airflow.cfg. It can be found "
            "under the [core] (<2.0.0) section or [logging] (>=2.0.0) in the cfg file. "
            "Kindly provide an appropriate directory path."
        )
    return folder


def _get_directories_to_delete(
    base_log_folder: str,
    enable_child_logs: bool,
) -> List[str]:
    """Return directories that should have logs cleaned."""

    directories: List[str] = [base_log_folder]
    if enable_child_logs:
        try:
            child_directory = conf.get("scheduler", "CHILD_PROCESS_LOG_DIRECTORY")
        except Exception as exc:  # pragma: no cover - airflow config access error
            logger.exception(
                "Could not obtain CHILD_PROCESS_LOG_DIRECTORY from Airflow Configurations: %s",
                exc,
            )
        else:
            if child_directory.strip():
                directories.append(child_directory)

    return directories


def _iter_cleanup_targets(
    number_of_workers: int, directories: Iterable[str]
) -> Iterable[Tuple[int, int, str]]:
    """Yield the worker index, directory index and path for cleanup tasks."""

    for log_cleanup_id in range(1, number_of_workers + 1):
        for dir_id, directory in enumerate(directories):
            yield log_cleanup_id, dir_id, str(directory)


DAG_ID = Path(__file__).with_suffix("").name
START_DATE = airflow.utils.dates.days_ago(1)
BASE_LOG_FOLDER = _get_base_log_folder()
SCHEDULE_INTERVAL = "@daily"
DAG_OWNER_NAME = "operations"
ALERT_EMAIL_ADDRESSES: List[str] = []
DEFAULT_MAX_LOG_AGE_IN_DAYS = int(
    Variable.get("airflow_log_cleanup__max_log_age_in_days", 30)
)
ENABLE_DELETE = True
NUMBER_OF_WORKERS = 1
ENABLE_DELETE_CHILD_LOG_RAW = Variable.get(
    "airflow_log_cleanup__enable_delete_child_log", "False"
)
ENABLE_DELETE_CHILD_LOG = _is_truthy(ENABLE_DELETE_CHILD_LOG_RAW)
LOG_CLEANUP_PROCESS_LOCK_FILE = "/tmp/airflow_log_cleanup_worker.lock"

logger.info("ENABLE_DELETE_CHILD_LOG %s", ENABLE_DELETE_CHILD_LOG_RAW)

DIRECTORIES_TO_DELETE = _get_directories_to_delete(
    BASE_LOG_FOLDER, ENABLE_DELETE_CHILD_LOG
)


default_args = {
    "owner": DAG_OWNER_NAME,
    "depends_on_past": False,
    "email": ALERT_EMAIL_ADDRESSES,
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    tags=["teamclairvoyant", "airflow-maintenance-dags"],
    template_undefined=jinja2.Undefined,
)

dag.doc_md = __doc__

log_cleanup_script = build_cleanup_bash_script(
    default_max_log_age_in_days=DEFAULT_MAX_LOG_AGE_IN_DAYS,
    enable_delete=ENABLE_DELETE,
    lock_file_path=LOG_CLEANUP_PROCESS_LOCK_FILE,
)


with dag:
    start = DummyOperator(task_id="start")

    for log_cleanup_id, dir_id, directory in _iter_cleanup_targets(
        NUMBER_OF_WORKERS, DIRECTORIES_TO_DELETE
    ):
        log_cleanup_op = BashOperator(
            task_id=f"log_cleanup_worker_num_{log_cleanup_id}_dir_{dir_id}",
            bash_command=log_cleanup_script,
            params={
                "directory": directory,
                "sleep_time": int(log_cleanup_id) * 3,
            },
        )

        start >> log_cleanup_op
