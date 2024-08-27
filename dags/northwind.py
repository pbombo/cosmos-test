from os import getenv
from pathlib import Path

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from pendulum import datetime

from cosmos import (
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
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

with DAG(
    dag_id="northwind_kubernetes",
    start_date=datetime(2022, 11, 27),
    doc_md=__doc__,
    catchup=False,
) as dag:
    run_models = DbtTaskGroup(
        project_config=ProjectConfig("/opt/airflow/dags/repo/dbt/northwind"),
        profile_config=ProfileConfig(
            profile_name="northwind",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dags/repo/dbt/northwind/dbt_profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
        ),
        operator_args={
            "image": DBT_IMAGE,
            "get_logs": True,
            "is_delete_operator_pod": False,
            "secrets": [trino_user_secret, trino_password_secret, trino_host_secret],
        },
    )
