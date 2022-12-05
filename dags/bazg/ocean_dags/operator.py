from collections import defaultdict
import json
from typing import Dict
import logging

from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task, task_group

from bazg.common.kafka_hooks import KafkaConsumerHook, KafkaProducerHook

logger = logging.getLogger(__name__)
TOPIC = "TopicA"

# @task(task_id="consume-upstream-data-updates", multiple_outputs=True, wait_for_downstream=True)
# def consume_upstream_data_changes(
#     this_data_object_id, upstream_data_object_ids, logger=logging.getLogger()
# ):
#     """Consume messages from Kafka topic and filter for updates in upstream
#     data objects"""
#     group_id = f"{this_data_object_id}-consumer"
#     config = {
#         "bootstrap.servers": "kafka:9092",
#         "group.id": group_id,
#         "client.id": f"airflow-dag-{this_data_object_id}",
#         "auto.offset.reset": "earliest",
#         # "heartbeat.interval.ms": 500,
#     }
#     consumer = KafkaConsumerHook(config=config).get_consumer()
#     consumer.subscribe([TOPIC])

#     total_msg_processed = 0
#     new_data = defaultdict(list)
#     while True:
#         messages = consumer.consume(num_messages=10, timeout=60)
#         if not messages:
#             logger.info(
#                 f"no more messages to consume for topic {TOPIC} and group ID {group_id}"
#             )
#             break
#         total_msg_processed += len(messages)
#         for m in messages:
#             this_msg_str = f"{m.topic()}[{m.partition()}]@{m.offset()}"
#             if m.error():
#                 logger.error(
#                     f"received message in {this_msg_str} with error {m.error()}"
#                 )
#                 raise RuntimeError(m.error())
#             logger.info(
#                 f"{this_data_object_id}: {m.topic()} @ {m.offset()}; {m.key()} :"
#                 f" {m.value()}"
#             )
#             try:
#                 data = json.loads(m.value().decode())
#                 updated_data_object_id = data.get("data_object_id")


#                 if updated_data_object_id in upstream_data_object_ids:
#                     new_data[updated_data_object_id].append(data)

#             except Exception as e:
#                 raise RuntimeError(
#                     f"error processing message {m.topic()}[{m.partition()}]@{m.offset()}"
#                 ) from e

#     # Done consuming and interpreting the new Kafka messages.
#     consumer.commit()
#     # NO FAILURES AFTER THIS POINT

#     return new_data


def consume_upstream_data_updates_func(
    this_data_object_id, upstream_data_object_ids, **kwargs
):
    """Consume messages from Kafka topic and filter for updates in upstream
    data objects"""
    group_id = f"{this_data_object_id}-consumer"
    config = {
        "bootstrap.servers": "kafka:9092",
        "group.id": group_id,
        "client.id": f"airflow-dag-{this_data_object_id}",
        "auto.offset.reset": "earliest",
        # "heartbeat.interval.ms": 500,
    }
    consumer = KafkaConsumerHook(config=config).get_consumer()
    consumer.subscribe([TOPIC])

    total_msg_processed = 0
    new_data = defaultdict(list)
    while True:
        messages = consumer.consume(num_messages=10, timeout=5)
        if not messages:
            logger.info(
                f"no more messages to consume for topic {TOPIC} and group ID {group_id}"
            )
            break
        total_msg_processed += len(messages)
        for m in messages:
            this_msg_str = f"{m.topic()}[{m.partition()}]@{m.offset()}"
            if m.error():
                logger.error(
                    f"received message in {this_msg_str} with error {m.error()}"
                )
                raise RuntimeError(m.error())
            logger.info(
                f"{this_data_object_id}: {m.topic()} @ {m.offset()}; {m.key()} :"
                f" {m.value()}"
            )
            try:
                data = json.loads(m.value().decode())
                updated_data_object_id = data.get("data_object_id")

                # data_source
                if (
                    not upstream_data_object_ids
                    and updated_data_object_id == this_data_object_id
                ):
                    new_data[updated_data_object_id].append(data)
                # data_object
                elif updated_data_object_id in upstream_data_object_ids:
                    new_data[updated_data_object_id].append(data)

            except Exception as e:
                raise RuntimeError(
                    f"error processing message {m.topic()}[{m.partition()}]@{m.offset()}"
                ) from e

    ti = kwargs["ti"]
    ti.xcom_push(key="new_data", value=new_data)

    # Done consuming and interpreting the new Kafka messages.
    consumer.commit()
    # NO FAILURES AFTER THIS POINT
    return


@task(task_id="publish-data-changes")
def publish_data_changes(this_data_object_id, run_id=None):
    """Consume messages from Kafka topic and filter for updates in upstream
    data objects"""
    config = {
        "bootstrap.servers": "kafka:9092",
        "client.id": f"airflow-dag-{this_data_object_id}",
    }
    producer = KafkaProducerHook(config=config).get_producer()

    producer.produce(
        TOPIC,
        key=this_data_object_id,
        value=json.dumps(
            {
                "data_object_id": this_data_object_id,
                "data_object_type": "iceberg",
                "updated_data_ids": run_id,
                "updated_data_field": "dag_run_id".encode(),
            }
        ).encode(),
    )
    producer.flush()


@task_group
def update_upstream_data_objects(
    this_data_object_id, upstream_data_object_ids, **context
):
    """Update upstream data objects."""
    tasks = [
        TriggerDagRunOperator(
            trigger_dag_id=doid,
            trigger_run_id=f"trigger-{this_data_object_id}-" + "{{ run_id }}",
            task_id=f"update-{doid}",
            wait_for_completion=True,
        )
        for doid in upstream_data_object_ids
    ]
    return tasks


# @task.branch(task_id="has-upstream-data-changed")
# def has_upstream_data_changed(upstream_data_changes: Dict):
#     if len(upstream_data_changes):
#         return "update-data"
#     else:
#         return None

# @task(task_id="update-data", multiple_outputs=True)
# def update_data(upstream_data_changes: Dict, **context):
#     print(upstream_data_changes)
#     return {"this_data_object_id": 1, "upstream_data_object_ids": [context.get("run_id")]}


def consume_changes_op_func(**kwargs):
    mock_messages = [
        {"object_id": 1, "config": "important"},
        {"object_id": 1, "config": "important"},
    ]
    print(f"pushing {mock_messages}")
    kwargs["ti"].xcom_push(key="messages", value=mock_messages)


def update_data_func(run_id, **kwargs):
    messages = kwargs["ti"].xcom_pull(
        task_ids="consume-upstream-data-updates", key="messages"
    )
    print(messages)
    kwargs["ti"].xcom_push(
        key="updated_ids",
        value={
            "this_data_object_id": self.data_object_id,
            "upstream_data_object_ids": run_id,
        },
    )


def has_upstream_data_changed_func(**kwargs):
    upstream_data_changes = kwargs["ti"].xcom_pull(
        task_ids="consume-upstream-data-updates", key="new_data"
    )
    if len(upstream_data_changes):
        return "update-data"
    else:
        return None
