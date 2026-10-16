
from datetime import datetime
from pathlib import Path

from airflow.sdk import BaseHook, Variable, dag, task
# from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt")

profile_config = ProfileConfig(
    profile_name="netflix",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT_PATH / "ci" / "profiles.yml",
)
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException

CONN_ID = "databricks_default"
BATCH_DIR = Path("/usr/local/airflow/include/data/ratings_batches")
VOLUME_DIR = "/Volumes/movielens/raw/netflixstage/ratings_batches"


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["movielens"],
)
def movielens_pipeline():
    @task
    def pick_batch() -> str:
        n = int(Variable.get("ratings_next_batch", default=1))
        name = f"ratings_batch_{n:03d}.csv"
        if not (BATCH_DIR / name).exists():
            raise AirflowSkipException("No more batches to load")
        return name

    @task
    def upload_batch(name: str) -> str:
        from databricks.sdk import WorkspaceClient

        conn = BaseHook.get_connection(CONN_ID)
        host = conn.host if conn.host.startswith("http") else f"https://{conn.host}"
        w = WorkspaceClient(host=host, token=conn.extra_dejson["token"])
        w.files.create_directory(VOLUME_DIR)
        with open(BATCH_DIR / name, "rb") as f:
            w.files.upload(f"{VOLUME_DIR}/{name}", f, overwrite=True)
        return name

    @task
    def load_batch(name: str) -> str:
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        hook = DatabricksSqlHook(
            databricks_conn_id=CONN_ID,
            http_path=Variable.get("DATABRICKS_HTTP_PATH"),
        )
        hook.run(f"""
            COPY INTO movielens.raw.raw_ratings
            FROM (
              SELECT CAST(userId AS INT) AS userId,
                     CAST(movieId AS INT) AS movieId,
                     CAST(rating AS DOUBLE) AS rating,
                     CAST(timestamp AS BIGINT) AS timestamp
              FROM '{VOLUME_DIR}/'
            )
            FILEFORMAT = CSV
            FILES = ('{name}')
            FORMAT_OPTIONS ('header' = 'true')
        """)
        return name

    @task
    def advance_pointer(name: str):
        n = int(name.split("_")[-1].split(".")[0])
        Variable.set("ratings_next_batch", str(n + 1))

    loaded = load_batch(upload_batch(pick_batch()))

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(emit_datasets=False),
        operator_args={"install_deps": True},
    )

    loaded >> dbt_build
    advance_pointer(loaded)

movielens_pipeline()
