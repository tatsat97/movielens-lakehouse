from datetime import datetime
from airflow.sdk import dag, task, Variable


@dag(start_date=datetime(2026, 1, 1), schedule=None, catchup=False, tags=["test"])
def test_databricks_connection():
    @task
    def check():
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        hook = DatabricksSqlHook(
            databricks_conn_id="databricks_default",
            http_path=Variable.get("DATABRICKS_HTTP_PATH"),
        )
        print(hook.get_first("select current_user(), current_catalog()"))

    check()


test_databricks_connection()