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
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator


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

        def consumer_function(message):
            """Consume messages from Kafka topic and filter for updates in upstream
            dependencies"""
            if message.key() is None:
                return False

            key = json.loads(message.key())
            value = json.loads(message.value())
            consumer_logger.info(
                f"{data_object_id}: {message.topic()} @ {message.offset()}; {key} :"
                f" {value}"
            )
            if data_object_id == key:
                return message
            return None

        # TODO: change consume operator to store msg offset-range in XCom and move
        # commit to new commit operator at the end of the DAG
        check_upstream_new_data = ConsumeFromTopicOperator(
            task_id="check-upstream-new-data",
            topics=["TopicA"],
            apply_function="data_object_dag_factory.consumer_function",
            consumer_config={
                "bootstrap.servers": "kafka:9092",
                "group.id": data_object_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
            },
            commit_cadence="end_of_operator",  # should be end of DAG
            max_messages=None,
            do_xcom_push=True,
            poll_timeout=5,
            xcom_return_key="results",
        )

        @task.branch(task_id="has-new-data")
        def has_new_data_branch(ti=None):
            check_upstream_results = ti.xcom_pull(
                task_ids="check-upstream-new-data", key="results"
            )
            if check_upstream_results is not None and any(check_upstream_results):
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

        data_object_type = data_object_definition.get("type")
        data_object_id_field = data_object_definition.get("id-field")

        def producer_function(updated_data_ids: list = []):
            """Notify downstream by producing message to Kafka."""

            yield json.dumps(data_object_id), json.dumps(
                {
                    "data_object_id": data_object_id,
                    "data_object_type": data_object_type,
                    "updated_data_ids": updated_data_ids,
                    "updated_data_field": data_object_id_field,
                    "downstream_kwargs": {},
                }
            )
            return

        notify_new_data = ProduceToTopicOperator(
            task_id="notify-new-data",
            topic="TopicA",
            producer_function="data_object_dag_factory.producer_function",
            producer_function_kwargs="",  # TODO: fill in templated XCom details from update task
            kafka_config={"bootstrap.servers": "kafka:9092"},
        )

        @task_group
        def update_dependencies(dependencies):
            """Update upstream data objects."""
            tasks = [
                TriggerDagRunOperator(
                    trigger_dag_id=d,
                    task_id=f"update-{d}",
                    wait_for_completion=True,
                    poke_interval=5,
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
