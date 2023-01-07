"""Example DAG demonstrating the usage of the BranchPythonOperator."""

import random
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='example_branch_operator',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=['example', 'example2'],
) as dag:
    run_this_first = EmptyOperator(
        task_id='run_this_first',
    )

    options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: random.choice(options),
    )
    run_this_first >> branching

    join = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for option in options:
        t = EmptyOperator(
            task_id=option,
        )

        empty_follow = EmptyOperator(
            task_id='follow_' + option,
        )

        # Label is optional here, but it can help identify more complex branches
        branching >> Label(option) >> t >> empty_follow >> join