from pendulum import datetime, duration
from bs4 import BeautifulSoup
import requests
import pandas as pd
from minio import Minio
from io import BytesIO

# DAG and task decorators for interfacing with the TaskFlow API
from airflow.decorators import dag, task, task_group

# A function that sets sequential dependencies between tasks including lists of tasks
from airflow import Dataset
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

# Used to label node edges in the Airflow UI
from airflow.utils.edgemodifier import Label

# Used to interact with Pharmaceutical Benefits Scheme
from include.hooks.pbs import PharmaceuticalBenefitsSchemeHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

"""
This DAG is intended to demonstrate a number of core Apache Airflow concepts that are central to the pipeline
authoring experience, including the TaskFlow API, Edge Labels, Jinja templating, branching,
generating tasks within a loop, task groups, and trigger rules.

First, this DAG checks if the current day is a weekday or weekend. Next, the DAG checks which day of the week
it is. Lastly, the DAG prints out a bash statement based on which day it is. On Tuesday, for example, the DAG
prints "It's Tuesday and I'm busy with studying".

This DAG uses the following operators:

EmptyOperator -
    Does nothing but can be used to structure your DAG.

    See more info about this operator here:
        https://registry.astronomer.io/providers/apache-airflow/modules/emptyoperator
"""

@task
def collect_csv_links(endpoint: str):
    hook = PharmaceuticalBenefitsSchemeHook()
    host = hook.get_conn().host.rstrip('/')
    response = requests.get(f'{host}{endpoint}')
    soup = BeautifulSoup(response.content, 'html.parser')
    links_with_csv = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.csv'):
            links_with_csv.append(href)
    return links_with_csv

@task
def create_bucket(name: str):
    
    minio_client = Minio(
        "host.docker.internal:9000", 
        access_key='minioadmin', 
        secret_key='minioadmin',
        secure=False) # Edit this bit to connect to your Minio server
    
    minio_client.make_bucket(name)
    


@task
def dump_pbs_data_from_links(links: list):
    pbs_hook = PharmaceuticalBenefitsSchemeHook()
    
    minio_client = Minio(
        "host.docker.internal:9000", 
        access_key='minioadmin', 
        secret_key='minioadmin',
        secure=False) # Edit this bit to connect to your Minio server

    pbs_host = pbs_hook.get_conn().host.rstrip('/')
    for endpoint in links:
        url_parts = endpoint.split('/')
        filename_with_extension = url_parts[-1]
        response = requests.get(f'{pbs_host}{endpoint}')
        
        minio_client.put_object(
            bucket_name='pharmaceutical-bureau-scheme',
            object_name=filename_with_extension,
            length=len(response.content),
            content_type='text/csv',
            data=BytesIO(response.content)
        )


@dag(
    start_date=datetime(2023, 1, 1),
    # This defines how many instantiations of this DAG (DAG Runs) can execute concurrently. In this case,
    # we're only allowing 1 DAG run at any given time, as opposed to allowing multiple overlapping DAG runs.
    max_active_runs=1,
    # This defines how often your DAG will run, or the schedule by which DAG runs are created. It can be
    # defined as a cron expression, custom timetable, existing presets or using the Dataset feature.
    # This DAG uses a preset to run monthly.
    schedule="@monthly",
    # Default settings applied to all tasks within the DAG; can be overwritten at the task level.
    default_args={
        "owner": "analytics",  # Defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 2,  # If a task fails, it will retry 2 times.
        "retry_delay": duration(
            minutes=3
        ),  # A task that fails will wait 3 minutes to retry.
    },
    default_view="graph",  # This defines the default view for this DAG in the Airflow UI
    # When catchup=False, your DAG will only run for the latest schedule interval. In this case, this means
    # that tasks will not be run between January 1st, 2023 and 1 day ago. When turned on, this DAG's first run
    # will be for today, per the @monthly schedule
    catchup=False,
    tags=["pbs"],  # If set, this tag is shown in the DAG view of the Airflow UI
)
def ingest_date_of_supply_data():
    # Last task will only trigger if all upstream tasks have succeeded or been skipped
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")
    create_bucket_op = create_bucket(name='pharmaceutical-bureau-scheme')
    collect_op = collect_csv_links(endpoint='/info/statistics/dos-and-dop/dos-and-dop')
    dump_op = dump_pbs_data_from_links(collect_op)
    
    create_bucket_op >> dump_op >> end

ingest_date_of_supply_data()