import datetime
from enum import Enum
from typing import List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from bazg.ocean_dags.operator import (
    consume_upstream_data_changes,
    update_upstream_data_objects,
    publish_data_changes,
    has_upstream_data_changed_func,
    consume_changes_op_func,
    update_data_func,
    consume_upstream_data_updates_func,
)


class DataObjectType(Enum):
    S3_PATH = 1
    ICEBERG_TABLE = 2
    POSTGRES_TABLE = 3


class DataObject(object):
    def __init__(
        self,
        data_object_id: str,
        data_object_type: DataObjectType,
        schedule: Optional[int] = None,
        upstream_data_object_ids: Optional[List[int]] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        super().__init__()
        self._data_object_id = data_object_id
        self._data_object_type = data_object_type
        self._schedule = (
            datetime.timedelta(seconds=schedule)
            if isinstance(schedule, int)
            else schedule
        )
        self._upstream_data_object_ids = (
            upstream_data_object_ids if upstream_data_object_ids else []
        )
        self._tags = tags if tags else []

    @property
    def data_object_id(self) -> str:
        return self._data_object_id

    @property
    def data_object_type(self) -> DataObjectType:
        return self._data_object_type

    @property
    def schedule(self):
        return self._schedule

    @property
    def upstream_data_object_ids(self) -> List[int]:
        return self._upstream_data_object_ids

    @property
    def tags(self) -> List[str]:
        return self._tags

    def dag(self) -> DAG:
        with DAG(
            dag_id=self.data_object_id,
            schedule=self.schedule,
            max_active_runs=1,
            start_date=datetime.datetime(2022, 11, 20),
            catchup=False,
            tags=self._tags,
            default_args={
                "depends_on_past": True,
                "provide_context": True,
            },
        ):
            # @task.branch(task_id="has-upstream-data-changed")
            # def has_upstream_data_changed(upstream_data_changes: Dict):
            #     if len(upstream_data_changes):
            #         return "update-data"
            #     else:
            #         return None

            has_upstream_data_changed = BranchPythonOperator(
                task_id="has-upstream-data-changed",
                python_callable=has_upstream_data_changed_func,
            )

            # consume_changes_op = consume_upstream_data_changes(
            #     this_data_object_id=self.data_object_id,
            #     upstream_data_object_ids=self.upstream_data_object_ids,
            # )

            # if self.upstream_data_object_ids:
            #     update_upstream_op = update_upstream_data_objects(
            #         this_data_object_id=self.data_object_id,
            #         upstream_data_object_ids=self.upstream_data_object_ids,
            #     )

            #     update_upstream_op >> consume_changes_op

            consume_data = PythonOperator(
                task_id="consume-upstream-data-updates",
                python_callable=consume_upstream_data_updates_func,
                wait_for_downstream=True,
                op_kwargs={
                    "this_data_object_id": self.data_object_id,
                    "upstream_data_object_ids": self.upstream_data_object_ids,
                },
            )

            update_data = PythonOperator(
                task_id="update-data",
                python_callable=update_data_func,
                op_kwargs={"run_id": "{{ run_id }}"},
            )

            consume_data >> has_upstream_data_changed >> update_data
