from typing import Dict
import datetime
import os
import yaml
import logging
from time import sleep

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task, task_group

from confluent_kafka import TopicPartition
from bazg.common.kafka_hooks import KafkaConsumerHook, KafkaProducerHook
from bazg.ocean_dags.operator import publish_data_changes, consume_upstream_data_changes
from bazg.ocean_dags.data_object import DataObject, DataObjectType

filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data_objects.yaml")
with open(filename, "rb") as infile:
    data_obj_spec = yaml.safe_load(infile)

for data_object_id, data_object_definition in data_obj_spec.items():
    schedule = data_object_definition.get("schedule")
    do = DataObject(
        data_object_id=data_object_id,
        data_object_type=DataObjectType[data_object_definition.get("types", "S3_PATH")],
        schedule = data_object_definition.get("schedule"),
        upstream_data_object_ids=data_object_definition.get("dependencies"),
        tags=data_object_definition.get("tags"),
    )
    this_do_dag = do.dag()

