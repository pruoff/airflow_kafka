from __future__ import annotations

import os
import yaml
from datetime import timedelta, datetime
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from bazg.common.kafka_hooks import KafkaProducerHook


with DAG(
    "produce_test_source_data",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start_operator = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    @task
    def generate_pseudo_data(topic):
        """Produces 5 messages to each of the data sources 1 to 4."""
        filename = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "data_objects.yaml"
        )
        producer = KafkaProducerHook(
            config={"bootstrap.servers": "kafka:9092"}
        ).get_producer()

        with open(filename, "rb") as infile:
            data_obj_spec = yaml.safe_load(infile)

        for data_object_id, data_object_definition in data_obj_spec.items():
            if "source" not in data_object_id:
                continue

            data_object_type = data_object_definition.get("type")
            data_object_id_field = data_object_definition.get("id-field")

            for i in range(5):
                new_data_notification_message = {
                    "data_object_id": data_object_id,
                    "data_object_type": data_object_type,
                    "updated_data_ids": [i],
                    "updated_data_field": data_object_id_field,
                    "downstream_kwargs": {"additional_arg_1": "value_about_new_data"},
                }

                producer.produce(
                    topic,
                    key=data_object_id,
                    value=json.dumps(new_data_notification_message),
                )
        producer.flush()

    start_operator >> generate_pseudo_data("TopicA")
