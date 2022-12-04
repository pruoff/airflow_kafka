import datetime
import os
from re import U
import yaml
import logging
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.edgemodifier import Label
from airflow.decorators import task, task_group

from bazg.ocean_dags.kafka_hooks import KafkaConsumerHook

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

        @task
        def check_new_data(topic, upstream_data_objects):
            """Consume messages from Kafka topic and filter for updates in upstream
            data objfects"""
            consumer = KafkaConsumerHook(
                config={
                    "bootstrap.servers": "kafka:9092",
                    "group.id": f"{data_object_id}-consumer",
                }
            ).get_consumer()
            consumer.subscribe(topic)

            messages = consumer.consume()
            for m in messages:
                key = json.loads(m.key())
                value = json.loads(m.value())
                consumer_logger.info(
                    f"{data_object_id}: {m.topic()} @ {m.offset()}; {key} :" f" {value}"
                )

            return len(messages)

        # TODO: change consume operator to store msg offset-range in XCom and move
        # move commit to new commit operator at the end of the DAG
        # check_upstream_new_data = ConsumeFromTopicOperator(
        #     task_id="check-upstream-new-data",
        #     topics=["TopicA"],
        #     apply_function="data_object_dag_factory.consumer_function",
        #     consumer_config={
        #         "bootstrap.servers": "kafka:9092",
        #         "group.id": data_object_id,
        #         "enable.auto.commit": False,
        #         "auto.offset.reset": "beginning",
        #     },
        #     commit_cadence="end_of_operator",  # should be end of DAG
        #     max_messages=None,
        #     do_xcom_push=True,
        #     poll_timeout=5,
        #     xcom_return_key="results",
        # )

        @task.branch(task_id="has-new-data")
        def has_new_data_branch(ti=None):
            check_upstream_results = ti.xcom_pull(
                task_ids="check-upstream-new-data", key="results"
            )
            if check_upstream_results is not None and any(check_upstream_results):
                return "update-data"
            else:
                return "ending"

        has_new_data = has_new_data_branch()
        update_data = BashOperator(
            task_id="update-data",
            bash_command="echo $xcom",
            env={"xcom": '{{ ti.xcom_pull(task_ids="check-upstream-new-data") }}'},
        )

        data_object_type = data_object_definition.get("type")
        data_object_id_field = data_object_definition.get("id-field")

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

        ending = EmptyOperator(task_id="ending")

        upstream_dependencies = data_object_definition.get("dependencies", [])
        check_new_data = check_new_data("TopicA", upstream_dependencies)
        # only for non-source data objects
        if upstream_dependencies:
            update_dependencies = update_dependencies(
                dependencies=upstream_dependencies
            )
            update_dependencies >> check_new_data

        check_new_data >> has_new_data
        has_new_data >> Label("new data") >> update_data >> notify_new_data >> ending
        has_new_data >> Label("no data") >> ending
