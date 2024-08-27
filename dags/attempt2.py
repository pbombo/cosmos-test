"""
A DAG that uses Cosmos with a custom profile.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

DEFAULT_DBT_ROOT_PATH = '/opt/airflow/dags/repo/dbt'
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROFILES_FILE_PATH = '/opt/airflow/dags/repo/dbt/northwind' #Path(DBT_ROOT_PATH, "northwind", "dbt_profiles.yml")


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def user_defined_profile() -> None:
    """
    A DAG that uses Cosmos with a custom profile.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        # project_config=ProjectConfig(
        #     DBT_ROOT_PATH / "nortwhind",
        # ),
        # # project_config=ProjectConfig("/opt/airflow/dags/repo/dbt/northwind"),
        # profile_config=ProfileConfig(
        #     profile_name="nortwhind",
        #     target_name="dev",
        #     profiles_yml_filepath=PROFILES_FILE_PATH,
        # ),
        project_config=ProjectConfig("/opt/airflow/dags/repo/dbt/northwind"),
        profile_config=ProfileConfig(
            profile_name="northwind",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dags/repo/dbt/northwind/dbt_profiles.yml",
        ),        
        operator_args={"append_env": True, "install_deps": True},
        default_args={"retries": 2},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


user_defined_profile()