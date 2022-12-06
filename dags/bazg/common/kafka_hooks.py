from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from confluent_kafka import Consumer, Producer

class KafkaConsumerHook(BaseHook):
    """
    A hook to create a Kafka Consumer
    """

    default_conn_name = "kafka_default"

    def __init__(
        self,
        kafka_conn_id: Optional[str] = None,
        config: Optional[Dict[Any, Any]] = None,
    ) -> None:
        super().__init__()

        self.kafka_conn_id = kafka_conn_id
        self.config: Dict[Any, Any] = config or {}

        if not self.config.get("group.id", None):
            raise AirflowException(
                "The 'group.id' parameter must be set in the config dictionary'. Got <None>"
            )

        if not (self.config.get("bootstrap.servers", None) or self.kafka_conn_id):
            raise AirflowException("One of config['bootsrap.servers'] or kafka_conn_id must be provided.")

        if self.config.get("bootstrap.servers", None) and self.kafka_conn_id:
            raise AirflowException("One of config['bootsrap.servers'] or kafka_conn_id must be provided.")

        self.extra_configs = {}
        if self.kafka_conn_id:
            conn = self.get_connection(self.kafka_conn_id)
            self.extra_configs = {"bootstrap.servers": conn}

    def get_consumer(self) -> Consumer:
        """
        Returns a Consumer that has been subscribed to topics.
        """
        consumer = Consumer({**self.extra_configs, **self.config})
        self.log.info(f"Consumer {consumer}")

        return consumer

class KafkaProducerHook(BaseHook):
    """
    A hook to create a Kafka Producer
    """

    default_conn_name = "kafka_default"

    def __init__(
        self,
        kafka_conn_id: Optional[str] = None,
        config: Optional[Dict[Any, Any]] = None,
    ) -> None:
        super().__init__()

        self.kafka_conn_id = kafka_conn_id
        self.config: Dict[Any, Any] = config or {}

        if not (self.config.get("bootstrap.servers", None) or self.kafka_conn_id):
            raise AirflowException("One of config['bootstrap.servers'] or kafka_conn_id must be provided.")

        if self.config.get("bootstrap.servers", None) and self.kafka_conn_id:
            raise AirflowException("One of config['bootstrap.servers'] or kafka_conn_id must be provided.")

        self.extra_configs = {}
        if self.kafka_conn_id:
            conn = self.get_connection(self.kafka_conn_id)
            self.extra_configs = {"bootstrap.servers": conn}
            self.log.info(
                f"Connection ID {self.kafka_conn_id} used for bootstrap servers,"
                + " {extra_configs} added to kafka config."
            )

    def get_producer(self) -> Producer:
        """
        Returns http session to use with requests.
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """

        producer = Producer({**self.extra_configs, **self.config})

        self.log.info(f"Producer {producer}")
        return producer