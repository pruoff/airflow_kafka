from __future__ import annotations

from datetime import timedelta, datetime
import json

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator


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

    def producer_function():
        """Produces 20 messages to each of the data sources 1 to 4."""
        for source_idx in range(4):
            for i in range(20):
                yield (json.dumps(f"source-object-{source_idx + 1}"), json.dumps(i))

    produce_operator = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic="TopicA",
        producer_function="produce_test_source_data.producer_function",
        kafka_config={"bootstrap.servers": "kafka:9092"},
    )

    done_operator = BashOperator(
        task_id="print_done",
        bash_command="echo 'done!!'",
        depends_on_past=False,
    )

    start_operator >> produce_operator >> done_operator
