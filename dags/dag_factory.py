import datetime
import os
import yaml
import logging
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task, task_group
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator


filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data_objects.yaml")
with open(filename, "rb") as infile:
    data_obj_spec = yaml.safe_load(infile)

for data_object_id, data_object_definition in data_obj_spec.items():
    schedule = data_object_definition.get("schedule")
    if schedule is not None:
        if isinstance(schedule, int):
            schedule = datetime.timedelta(seconds=schedule)
    with DAG(
        data_object_id,
        schedule=schedule,
        max_active_runs=1,
        start_date=datetime.datetime(2022, 11, 20),
        catchup=False,
        tags=data_object_definition.get("tags", []),
        default_args={
            "depends_on_past": True,
        },
    ) as dag:

        consumer_logger = logging.getLogger("airflow")

        def consumer_function(message, **kwargs):
            """Consume messages from Kafka topic and filter for updates in upstream
            dependencies"""
            # TODO: implement message format and filter
            if message.key() is None:
                return False

            key = json.loads(message.key())
            value = json.loads(message.value())
            consumer_logger.info(
                f"{data_object_id}: {message.topic()} @ {message.offset()}; {key} :"
                f" {value}"
            )
            # kwargs['ti'].xcom_push(key=data_object_id, value=True)
            return True

        # TODO: change consume operator to store msg offset-range in XCom and move
        # commit to new commit operator at the end of the DAG
        check_upstream_new_data = ConsumeFromTopicOperator(
            task_id="check-upstream-new-data",
            topics=["TopicA"],
            apply_function="dag_factory.consumer_function",
            consumer_config={
                "bootstrap.servers": "kafka:9092",
                "group.id": data_object_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
            },
            commit_cadence="end_of_operator",  # should be end of DAG
            max_messages=None,
            do_xcom_push=True,
        )

        @task.branch(task_id="has-new-data")
        def has_new_data_branch(ti=None):
            # TODO: try:? for when no True was written?
            if ti.xcom_pull(task_ids="check-upstream-new-data"):
                return "update-data"
            else:
                return "no-op"

        has_new_data = has_new_data_branch()
        noop = EmptyOperator(task_id="no-op")
        update_data = BashOperator(
            task_id="update-data",
            bash_command="echo $xcom",
            env={"xcom": '{{ ti.xcom_pull(task_ids="check-upstream-new-data") }}'},
        )
        notify_new_data = EmptyOperator(task_id="notify-new-data")

        @task_group
        def update_dependencies(dependencies):
            """Update upstream data objects."""
            tasks = [
                TriggerDagRunOperator(
                    trigger_dag_id=d,
                    task_id=f"update-{d}",
                    wait_for_completion=True,
                )
                for d in dependencies
            ]
            return tasks

        dependencies = data_object_definition.get("dependencies", [])
        if dependencies:
            data_dependencies = update_dependencies(dependencies=dependencies)
            data_dependencies >> check_upstream_new_data

        check_upstream_new_data >> has_new_data
        has_new_data >> Label("new data") >> update_data >> notify_new_data
        has_new_data >> Label("no data") >> noop
