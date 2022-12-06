from collections import defaultdict
import json
import logging
from typing import Optional, List

from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task_group

from bazg.common.kafka_hooks import KafkaConsumerHook, KafkaProducerHook

TOPIC = "TopicA"


class GatherUpstreamDataChangesOperator(PythonOperator):
    ui_color = "#f6dff0"

    def __init__(
        self,
        this_data_object_id: str,
        upstream_data_object_ids: Optional[List[str]] = None,
        topic: str = TOPIC,
        task_id="gather-upstream-data-changes",
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=self.gather_upstream_data_changes,
            task_id=task_id,
            **kwargs,
        )
        self._this_data_object_id = this_data_object_id
        self._topic = topic
        self._upstream_data_object_ids = (
            upstream_data_object_ids if upstream_data_object_ids else []
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def gather_upstream_data_changes(self, task_instance=None):
        """Consume messages from Kafka topic and filter for updates in upstream
        data objects"""
        group_id = f"{self._this_data_object_id}-consumer"
        config = {
            "bootstrap.servers": "kafka:9092",
            "group.id": group_id,
            "client.id": f"airflow-dag-{self._this_data_object_id}",
            "auto.offset.reset": "earliest",
        }
        consumer = KafkaConsumerHook(config=config).get_consumer()
        consumer.subscribe([self._topic])

        total_msg_processed = 0
        data_changes = defaultdict(list)
        while True:
            messages = consumer.consume(num_messages=10, timeout=5)
            if not messages:
                self.logger.info(
                    f"no more messages to consume for topic {TOPIC} and group ID {group_id}"
                )
                break
            total_msg_processed += len(messages)
            for m in messages:
                this_msg_str = f"{m.topic()}[{m.partition()}]@{m.offset()}"
                if m.error():
                    self.logger.error(
                        f"received message in {this_msg_str} with error {m.error()}"
                    )
                    raise RuntimeError(m.error())
                self.logger.info(
                    f"{self._this_data_object_id}: {m.topic()} @ {m.offset()}; {m.key()} :"
                    f" {m.value()}"
                )
                try:
                    data = json.loads(m.value().decode())
                    updated_data_object_id = data.get("data_object_id")

                    if updated_data_object_id in self._upstream_data_object_ids:
                        data_changes[updated_data_object_id].append(data)
                except Exception as e:
                    raise RuntimeError(
                        f"error processing message {m.topic()}[{m.partition()}]@{m.offset()}"
                    ) from e

        task_instance.xcom_push(key="data_changes", value=data_changes)

        # Done consuming and interpreting the new Kafka messages.
        consumer.commit()
        # NO FAILURES AFTER THIS POINT
        return data_changes


class PublishDataChangesOperator(PythonOperator):
    ui_color = "#dff6f2"

    def __init__(
        self,
        this_data_object_id: str,
        task_id: str = "publish-data-changes",
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=self.publish_data_changes,
            task_id=task_id,
            **kwargs,
        )
        self._this_data_object_id = this_data_object_id

    def publish_data_changes(self, run_id=None):
        """Consume messages from Kafka topic and filter for updates in upstream
        data objects"""
        config = {
            "bootstrap.servers": "kafka:9092",
            "client.id": f"airflow-dag-{self._this_data_object_id}",
        }
        producer = KafkaProducerHook(config=config).get_producer()

        producer.produce(
            TOPIC,
            key=self._this_data_object_id.encode(),
            value=json.dumps(
                {
                    "data_object_id": self._this_data_object_id,
                    "data_object_type": "iceberg",
                    "updated_data_ids": run_id,
                    "updated_data_field": "dag_run_id",
                }
            ).encode(),
        )
        producer.flush()


@task_group
def update_upstream_data_objects(this_data_object_id, upstream_data_object_ids):
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


def update_data_func(task_instance=None, run_id=None, dag=None):
    messages = task_instance.xcom_pull(
        task_ids="gather-upstream-data-changes", key="data_changes"
    )
    print(messages)
    task_instance.xcom_push(
        key="updated_ids",
        value={
            "this_data_object_id": dag.dag_id,
            "upstream_data_object_ids": run_id,
        },
    )


class HasUpstreamDataChangedOperator(BranchPythonOperator):
    ui_color = "#eaf2d4"
    custom_operator_name = "HasUpstreamDataChangedOperator"

    def __init__(
        self,
        task_id: str = "has-upstream-data-changed",
        xcom_task_id: str = "gather-upstream-data-changes",
        xcom_key: str = "data_changes",
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id,
            python_callable=self.has_upstream_data_changed,
            **kwargs,
        )
        self._xcom_task_id = xcom_task_id
        self._xcom_key = xcom_key

    def has_upstream_data_changed(self, task_instance=None):
        upstream_data_changes = task_instance.xcom_pull(
            task_ids=self._xcom_task_id, key=self._xcom_key
        )
        if len(upstream_data_changes) > 0:
            return "update-data"
        else:
            return "data-object-update-done"
