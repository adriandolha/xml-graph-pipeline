import os

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import dates
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup
from uniprot import pipeline
import warnings

warnings.filterwarnings("ignore")

from airflow import DAG
from airflow.models import Variable, Pool, XCom
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List
import xml.etree.ElementTree as ET


@dataclass
class MyDataModel:
    id: int
    name: str


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 17),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def parse_xml(**kwargs):
    file_id = kwargs['file_id']
    file = kwargs["ti"].xcom_pull(key=file_id, task_ids=['parse_data_task_v5'])[0]
    print(file_id, file)
    data = pipeline.parse_xml(file)
    return pipeline.data_to_dict(data)


def load_data(**kwargs):
    task_id = kwargs['parse_data_task_id']

    validated_data = kwargs['ti'].xcom_pull(task_ids=task_id)
    print(validated_data)
    pipeline.load(pipeline.data_from_dict(validated_data))
    file_id = kwargs['file_id']
    file = kwargs["ti"].xcom_pull(key=file_id, task_ids=['parse_data_task_v5'])[0]
    pipeline.move_parsed_xml(file)


def move_parsed_xml(**kwargs):
    file_path = kwargs['file_path']
    pipeline.move_parsed_xml(file_path)


with DAG('uniprot_pipeline',
         default_args=default_args,
         description='DAG for processing XML files',
         start_date=dates.days_ago(1),
         catchup=False,
         ) as dag:
    pool = Pool(
        pool='uniprot_pool',
        slots=2,
        description='Pool for running tasks related to UniProt data processing'
    )

    directory_path = os.path.expanduser('~/uniprot_data')
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=directory_path,
        poke_interval=5,
        dag=dag
    )

    trigger_dag_run = TriggerDagRunOperator(
        task_id='trigger_dag_run',
        trigger_dag_id='uniprot_pipeline',
        pool=pool.pool,
        dag=dag
    )


    def parse_data(**kwargs):
        directory_path = os.path.expanduser('~/uniprot_data')
        files = os.listdir(directory_path)
        files.sort()
        tasks = []
        for index, file in enumerate(files[:2]):
            print(files[0])
            kwargs["ti"].xcom_push(key=f"file{index}", value=f'{directory_path}/{files[index]}')
            tasks.extend([f'parse_xml_task{index}', f'load_data_task{index}'])
        return tasks


    parse_data_task = BranchPythonOperator(
        task_id='parse_data_task_v5',
        python_callable=parse_data,
        dag=dag,
    )
    wait_for_file >> parse_data_task
    for i in range(2):
        parse_xml_task = PythonOperator(
            task_id=f'parse_xml_task{i}',
            python_callable=parse_xml,
            op_kwargs={'file_id': f'file{i}'},
            dag=dag,
        )
        load_data_task = PythonOperator(
            task_id=f'load_data_task{i}',
            python_callable=load_data,
            op_kwargs={'file_id': f'file{i}', 'parse_data_task_id': f'parse_xml_task{i}'},
            dag=dag,
        )
        parse_data_task >> parse_xml_task >> load_data_task >> trigger_dag_run
