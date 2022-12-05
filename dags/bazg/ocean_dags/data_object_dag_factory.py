import os
import yaml
# This is need in order for airflow to pick up this file during DAG collection.
from airflow import DAG

from bazg.ocean_dags.data_object import DataObject, DataObjectType

filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data_objects.yaml")
with open(filename, "rb") as infile:
    data_obj_spec = yaml.safe_load(infile)

for data_object_id, data_object_definition in data_obj_spec.items():
    schedule = data_object_definition.get("schedule")
    do_type = DataObjectType[data_object_definition.get("type", "S3_PATH").upper()]
    do = DataObject(
        data_object_id=data_object_id,
        data_object_type=do_type,
        schedule = data_object_definition.get("schedule"),
        upstream_data_objects=data_object_definition.get("dependencies"),
        tags=data_object_definition.get("tags", []) + [do_type.name],
    )
    this_do_dag = do.dag()

