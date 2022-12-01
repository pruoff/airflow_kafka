from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task, task_group
import datetime
import os
import yaml

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
        default_args={"depends_on_past": True}
    ) as dag:

        @task.branch(task_id="has-new-data")
        def has_new_data_branch(**context):
            if bool(context['ti'].xcom_pull(task_ids="get-new-data")):
                return "update-data"
            else:
                return "noop"

        @task_group
        def update_dependencies(dependencies):
            """Update upstream data objects."""
            tasks = [
                TriggerDagRunOperator(trigger_dag_id=d, task_id=f"update-{d}")
                for d in dependencies
            ]
            return tasks

        data_dependencies = update_dependencies(
            data_object_definition.get("dependencies", [])
        )
        get_new_data = BashOperator(task_id="get-new-data", bash_command="echo 'getting new data'")
        has_new_data = has_new_data_branch()
        noop = EmptyOperator(task_id="no-op")
        update_data = BashOperator(task_id="update-data", bash_command="echo $xcom", env={'xcom': '{{ ti.xcom_pull(task_ids="get-new-data") }}'})
        notify_new_data = EmptyOperator(task_id="notify-new-data")

        data_dependencies >> get_new_data >> has_new_data
        has_new_data >> Label("new data") >> update_data >> notify_new_data
        has_new_data >> Label("no data") >> noop
