from __future__ import annotations

import logging
from datetime import timedelta, datetime
import json

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator


with DAG(
    'kafka_await_dag',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
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

    consumer_logger = logging.getLogger("airflow")
    def consumer_function(message, prefix=None):
        key = json.loads(message.key())
        value = json.loads(message.value())
        consumer_logger.info(f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
        return

    def await_function(message):
        if json.loads(message.value()) % 5 == 0:
            return f" Got the following message: {json.loads(message.value())}"

    await_operator = AwaitKafkaMessageOperator(
        task_id="awaiting_message",
        topics=["TopicA"],
        apply_function="kafka_await_dag.await_function",
        kafka_config={
            "bootstrap.servers": "kafka:9092",
            "group.id": "awaiting_message",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
    )

    consume_operator = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["TopicA"],
        apply_function="kafka_await_dag.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "kafka:9092",
            "group.id": "awaiting_message",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    done_operator = BashOperator(
        task_id='print_done',
        bash_command="echo 'done!!'",
        depends_on_past=False,
    )

    await_operator >> consume_operator >> done_operator
