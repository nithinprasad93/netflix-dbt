"""
tmdb_pipeline.py
================
Daily Airflow DAG that orchestrates the full TMDB enrichment pipeline:

    1. extract_tmdb        — Reads tmdb_ids from Snowflake dim_movies, calls the
                             TMDB REST API for each movie, returns enriched metadata.

    2. load_to_snowflake   — Receives the API payload via XCom, creates the target
                             raw table if it doesn't exist, and upserts each movie
                             using a MERGE statement (idempotent — safe to re-run).

    3. dbt_transformations — Cosmos DbtTaskGroup that reads the dbt project and
                             generates one Airflow task per dbt node (model, test,
                             seed, snapshot). Tasks run in parallel where dbt
                             dependencies allow, respecting the full DAG lineage.

Credentials are injected via Docker Compose environment variables from airflow/.env.
The Snowflake connection for Cosmos is defined as AIRFLOW_CONN_SNOWFLAKE_DEFAULT
in docker-compose.yml so it survives container restarts without manual UI setup.

Schedule: daily (@daily). catchup=False means only the latest interval runs
on startup — historical backfills are skipped.
"""

import os
import requests
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cosmos is Astronomer's open-source library that converts a dbt project into
# an Airflow DAG. DbtTaskGroup creates one task per dbt node inside a task group.
# ProfileConfig tells Cosmos how to connect to the data warehouse.
# SnowflakeUserPasswordProfileMapping maps an Airflow connection to a dbt profile.
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# ── Default arguments applied to every task in this DAG ──────────────────────
# retries=1 means each task gets one automatic retry before marking as failed.
# retry_delay gives Snowflake / the API time to recover before the retry fires.
default_args = {
    'owner': 'nithin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── dbt project path inside the Airflow container ────────────────────────────
# The netflix/ dbt project is mounted into the container via docker-compose.yml:
#   - ../netflix:/opt/airflow/dbt/netflix
# Cosmos reads the project from this path at DAG parse time to build the task graph.
DBT_PROJECT_PATH = Path('/opt/airflow/dbt/netflix')
DBT_PROFILES_PATH = Path('/opt/airflow/dbt')

# ── Cosmos profile configuration ─────────────────────────────────────────────
# Instead of reading profiles.yml directly, Cosmos uses an Airflow Connection.
# AIRFLOW_CONN_SNOWFLAKE_DEFAULT is set as an environment variable in
# docker-compose.yml so the connection is always available without manual UI setup.
# profile_args overrides the schema to 'marts' — the production target schema.
profile_config = ProfileConfig(
    profile_name='netflix',
    target_name='prod',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_default',
        profile_args={'schema': 'marts'},
    ),
)

# ── DAG definition ────────────────────────────────────────────────────────────
# schedule_interval='@daily' runs at midnight UTC every day.
# start_date is required by Airflow but catchup=False prevents it from
# retroactively scheduling runs for every day since 2024-01-01.
with DAG(
    dag_id='tmdb_pipeline',
    default_args=default_args,
    description='Fetch TMDB metadata and load to Snowflake, then run dbt',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # ── Task 1: extract_tmdb ──────────────────────────────────────────────────
    # Connects to Snowflake to get the list of tmdb_ids that already exist in
    # dim_movies (populated from the original MovieLens dataset). For each id,
    # it calls the TMDB v3 API to retrieve enriched metadata: overview, runtime,
    # popularity, budget, revenue, release_date, poster_path.
    #
    # The function returns a list of JSON dicts. Airflow automatically stores
    # this return value in XCom so the next task can retrieve it.
    #
    # Authentication: TMDB requires a Bearer token (JWT), not just an API key.
    # The token is stored in TMDB_BEARER_TOKEN env var, injected via .env file.
    #
    # LIMIT 100 keeps the API call count manageable. In production this would
    # be replaced with incremental logic — only fetch movies added since the
    # last successful run.
    def extract_tmdb():
        import snowflake.connector

        # Connect to Snowflake using credentials from environment variables.
        # Never hardcode credentials — always read from env vars or a secrets manager.
        conn = snowflake.connector.connect(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            role=os.environ.get('SNOWFLAKE_ROLE'),
        )

        # Fetch tmdb_ids from the marts layer — only movies that have a TMDB id.
        # NULL tmdb_ids exist in the MovieLens dataset for older/obscure movies.
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT tmdb_id FROM marts.dim_movies WHERE tmdb_id IS NOT NULL LIMIT 100")
        tmdb_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        # Call the TMDB API for each movie.
        # The Authorization header uses the Read Access Token (Bearer JWT),
        # not the API key. TMDB v3 endpoints require Bearer auth.
        bearer_token = os.environ.get('TMDB_BEARER_TOKEN')
        headers = {'Authorization': f'Bearer {bearer_token}'}
        movies = []
        for tmdb_id in tmdb_ids:
            response = requests.get(
                f'https://api.themoviedb.org/3/movie/{tmdb_id}',
                headers=headers
            )
            # Only append successful responses. 404s occur for movies that have
            # been removed from TMDB — these are silently skipped.
            if response.status_code == 200:
                movies.append(response.json())

        # Returning the list stores it in XCom automatically.
        # The next task retrieves it via ti.xcom_pull(task_ids='extract_tmdb').
        return movies

    extract_task = PythonOperator(
        task_id='extract_tmdb',
        python_callable=extract_tmdb,
    )

    # ── Task 2: load_to_snowflake ─────────────────────────────────────────────
    # Pulls the movie list from XCom, creates raw.raw_tmdb_movies if it doesn't
    # exist, then upserts each movie using a Snowflake MERGE statement.
    #
    # MERGE (upsert) is used instead of INSERT to make this task idempotent:
    # running it multiple times with the same data produces the same result.
    # This is critical for safe retries — if the task fails halfway through,
    # rerunning it won't create duplicates.
    #
    # raw_json VARIANT column stores the full API response as JSON. This is a
    # Snowflake best practice for raw ingestion — store everything, extract
    # what you need in dbt staging models. Allows schema evolution without
    # pipeline changes.
    #
    # provide_context=True passes the Airflow task instance (ti) as a kwarg,
    # which is needed to call ti.xcom_pull().
    def load_to_snowflake(**context):
        import snowflake.connector
        import json

        # ti (task instance) is the Airflow object that provides access to XCom.
        ti = context['ti']
        movies = ti.xcom_pull(task_ids='extract_tmdb')

        if not movies:
            print("No movies to load")
            return

        conn = snowflake.connector.connect(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            role=os.environ.get('SNOWFLAKE_ROLE'),
        )

        cursor = conn.cursor()

        # Create the raw table if it doesn't exist.
        # This makes the pipeline self-bootstrapping — no manual DDL required.
        # loaded_at tracks when each row was last refreshed, useful for debugging
        # and incremental logic in downstream dbt models.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_tmdb_movies (
                tmdb_id        NUMBER,
                title          VARCHAR,
                overview       VARCHAR,
                runtime        NUMBER,
                popularity     FLOAT,
                budget         NUMBER,
                revenue        NUMBER,
                release_date   VARCHAR,
                poster_path    VARCHAR,
                raw_json       VARIANT,
                loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # MERGE upserts each movie by tmdb_id.
        # WHEN MATCHED → update all fields (handles TMDB data corrections over time)
        # WHEN NOT MATCHED → insert new row
        # %s placeholders are safely parameterised by snowflake-connector-python,
        # preventing SQL injection.
        for movie in movies:
            cursor.execute("""
                MERGE INTO raw.raw_tmdb_movies AS target
                USING (SELECT %s AS tmdb_id) AS source
                ON target.tmdb_id = source.tmdb_id
                WHEN MATCHED THEN UPDATE SET
                    title        = %s,
                    overview     = %s,
                    runtime      = %s,
                    popularity   = %s,
                    budget       = %s,
                    revenue      = %s,
                    release_date = %s,
                    poster_path  = %s,
                    raw_json     = PARSE_JSON(%s),
                    loaded_at    = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN INSERT
                    (tmdb_id, title, overview, runtime, popularity, budget, revenue, release_date, poster_path, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
            """, (
                movie.get('id'),
                movie.get('title'),
                movie.get('overview'),
                movie.get('runtime'),
                movie.get('popularity'),
                movie.get('budget'),
                movie.get('revenue'),
                movie.get('release_date'),
                movie.get('poster_path'),
                json.dumps(movie),
                # INSERT values repeated — MERGE requires both MATCHED and NOT MATCHED params
                movie.get('id'),
                movie.get('title'),
                movie.get('overview'),
                movie.get('runtime'),
                movie.get('popularity'),
                movie.get('budget'),
                movie.get('revenue'),
                movie.get('release_date'),
                movie.get('poster_path'),
                json.dumps(movie),
            ))

        cursor.close()
        conn.close()
        print(f"Loaded {len(movies)} movies to Snowflake")

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    # ── Task 3: dbt_transformations (Cosmos DbtTaskGroup) ────────────────────
    # Cosmos reads the dbt project manifest and generates one Airflow task per
    # dbt node: each model gets a .run and .test task, seeds get a seed task,
    # and snapshots get a snapshot task. Dependencies between tasks mirror the
    # dbt DAG lineage exactly.
    #
    # Tasks run in parallel wherever dbt allows — e.g. all staging models run
    # simultaneously since they have no dependencies on each other.
    #
    # install_deps=True runs `dbt deps` before each task to ensure packages
    # (dbt_utils, dbt_expectations) are available inside the temp directory
    # Cosmos clones the project into.
    #
    # dbt_executable_path points to dbt installed inside the container via
    # the custom Dockerfile + requirements.txt.
    dbt_task_group = DbtTaskGroup(
        group_id='dbt_transformations',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path='/home/airflow/.local/bin/dbt',
        ),
        operator_args={'install_deps': True},
    )

    # ── Task dependency chain ─────────────────────────────────────────────────
    # >> is Airflow's bitshift operator for setting task dependencies.
    # extract_tmdb must succeed before load_to_snowflake runs.
    # load_to_snowflake must succeed before any dbt task runs.
    # Within dbt_transformations, Cosmos manages internal dependencies automatically.
    extract_task >> load_task >> dbt_task_group
