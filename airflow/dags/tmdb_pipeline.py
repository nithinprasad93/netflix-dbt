import os
import requests
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

default_args = {
    'owner': 'nithin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DBT_PROJECT_PATH = Path('/opt/airflow/dbt/netflix')
DBT_PROFILES_PATH = Path('/opt/airflow/dbt')

profile_config = ProfileConfig(
    profile_name='netflix',
    target_name='prod',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_default',
        profile_args={'schema': 'marts'},
    ),
)

with DAG(
    dag_id='tmdb_pipeline',
    default_args=default_args,
    description='Fetch TMDB metadata and load to Snowflake, then run dbt',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def extract_tmdb():
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            role=os.environ.get('SNOWFLAKE_ROLE'),
        )

        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT tmdb_id FROM marts.dim_movies WHERE tmdb_id IS NOT NULL LIMIT 100")
        tmdb_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        bearer_token = os.environ.get('TMDB_BEARER_TOKEN')
        headers = {'Authorization': f'Bearer {bearer_token}'}
        movies = []
        for tmdb_id in tmdb_ids:
            response = requests.get(
                f'https://api.themoviedb.org/3/movie/{tmdb_id}',
                headers=headers
            )
            if response.status_code == 200:
                movies.append(response.json())

        return movies

    extract_task = PythonOperator(
        task_id='extract_tmdb',
        python_callable=extract_tmdb,
    )

    def load_to_snowflake(**context):
        import snowflake.connector
        import json

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

    dbt_task_group = DbtTaskGroup(
        group_id='dbt_transformations',
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path='/home/airflow/.local/bin/dbt',
        ),
        operator_args={'install_deps': True},
    )

    extract_task >> load_task >> dbt_task_group
