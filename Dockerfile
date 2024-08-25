FROM python:3.12

# install dbt into a virtual environment
RUN python -m venv dbt_venv && . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core==1.8.3 dbt-trino==1.8.1 dbt-postgres==1.8.2 psycopg2==2.9.9 && deactivate

USER 1001
