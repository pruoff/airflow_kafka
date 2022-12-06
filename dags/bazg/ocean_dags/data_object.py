import datetime
from enum import Enum
from typing import List, Optional, Dict, Set

from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
)

from bazg.ocean_dags.operator import (
    update_data_func,
    update_upstream_data_objects,
    HasUpstreamDataChangedOperator,
    GatherUpstreamDataChangesOperator,
    PublishDataChangesOperator,
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
        upstream_data_objects: Optional[Dict[str, bool]] = None,
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
        self._upstream_data_objects = (
            upstream_data_objects if upstream_data_objects else {}
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
    def upstream_data_object_ids(self) -> Set[str]:
        return set(self._upstream_data_objects.keys())

    @property
    def upstream_data_object_ids_to_update(self) -> Set[str]:
        return {
            doid
            for doid, requires_update in self._upstream_data_objects.items()
            if requires_update
        }

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
            consume_data = GatherUpstreamDataChangesOperator(
                this_data_object_id=self.data_object_id,
                upstream_data_object_ids=self.upstream_data_object_ids,
                wait_for_downstream=True,
            )

            has_upstream_data_changed = HasUpstreamDataChangedOperator()

            if self.upstream_data_object_ids_to_update:
                update_upstream_op = update_upstream_data_objects(
                    this_data_object_id=self.data_object_id,
                    upstream_data_object_ids=self.upstream_data_object_ids_to_update,
                )

                update_upstream_op >> consume_data

            update_data = PythonOperator(
                task_id="update-data",
                python_callable=update_data_func,
                op_kwargs={"run_id": "{{ run_id }}"},
            )

            publish_changes = PublishDataChangesOperator(
                this_data_object_id=self.data_object_id
            )
            consume_data >> has_upstream_data_changed >> update_data >> publish_changes
