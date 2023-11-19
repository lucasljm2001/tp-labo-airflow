from __future__ import annotations

import datetime
import requests
from airflow.operators.python import PythonOperator

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="mi_ejemplo_bash",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    def obtener_personajes(page):
        url = f'https://rickandmortyapi.com/api/character?page={page}'
        response = requests.get(url)
        data = response.json()
        print(data)
        return data

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command='echo "hello world"',
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    # [START howto_operator_bash_template]
    for page in range(5):
        also_run_this = PythonOperator(
            task_id=f'obtener_pagina_{page}',
            python_callable=obtener_personajes,
            op_args=[page],
            provide_context=True,
            dag=dag,
        )
    # [END howto_operator_bash_template]
    also_run_this >> run_this_last


if __name__ == "__main__":
    dag.test()
