from os import getenv
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.secret import Secret

from pendulum import datetime, duration

from cosmos import (
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    ExecutionMode,
    DbtTaskGroup,
)


PROJECT_DIR = Path("dbt/northwind")
DBT_IMAGE = getenv('DBT_IMAGE')

trino_user_secret = Secret(
    deploy_type="env",
    deploy_target="TRINO_USER",
    secret="trino-secrets",
    key="username",
)

trino_password_secret = Secret(
    deploy_type="env",
    deploy_target="TRINO_PASSWORD",
    secret="trino-secrets",
    key="password",
)

trino_host_secret = Secret(
    deploy_type="env",
    deploy_target="TRINO_HOST",
    secret="trino-secrets",
    key="host",
)

# with DAG(
#     dag_id="northwind_kubernetes",
#     start_date=datetime(2022, 11, 27),
#     doc_md=__doc__,
#     catchup=False,
# ) as dag:
def create_dbt_dag(dag_id, schedule, select_tag, start_date=datetime(2024, 9, 1)):
    # Convert schedule to string if it's a Duration object
    if isinstance(schedule, duration):
        schedule = f"@{schedule.in_words()}"
        
    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False,
    ) as dag:
        pre_dbt = EmptyOperator(task_id="pre_dbt")
        dbt_task_group = DbtTaskGroup(
            project_config=ProjectConfig("/opt/airflow/dags/repo/dbt/northwind"),
            profile_config=ProfileConfig(
                profile_name="northwind",
                target_name="dev",
                profiles_yml_filepath="/opt/airflow/dags/repo/dbt/northwind/dbt_profiles.yml",
            ),
            execution_config=ExecutionConfig(
                execution_mode=ExecutionMode.LOCAL,
            ),
            operator_args={"append_env": True, "install_deps": True},
            default_args={"retries": 2},
            render_config=RenderConfig(select=select_tag),
        )

        pre_dbt >> dbt_task_group

        return dag


# DAG for models that run every 10 minutes
globals()['dag_10min'] = create_dbt_dag("lakehouse_etl_10min",
                          '@every 10 minutes', ['tag:first_tag'])

# DAG for models that run every 5 minutes
globals()['dag_5min'] = create_dbt_dag("lakehouse_etl_5min",
                           '@every 5 minutes', ['tag:some_tag'])