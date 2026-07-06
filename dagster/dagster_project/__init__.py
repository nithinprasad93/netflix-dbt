"""
dagster_project/__init__.py
============================
Top-level Dagster Definitions for the Netflix / MovieLens pipeline.

This is the entry point that `dagster dev` imports. The module_name in
pyproject.toml points here:

    [tool.dagster]
    module_name = "dagster_project"

Dagster looks for a variable named `defs` of type dagster.Definitions in this
module. Definitions is the central registry that wires together:
  - assets    : what data to produce (dbt models + TMDB ingestion)
  - resources : how to connect to external systems (Snowflake, dbt CLI)
  - schedules : when to run (daily at 06:00 UTC)
  - jobs      : which assets to run together

Loading .env before anything else
──────────────────────────────────
load_dotenv() is called at the top of this file, before any Dagster imports that
might try to read EnvVar values. Python-dotenv reads dagster/.env and populates
os.environ with SNOWFLAKE_* and TMDB_BEARER_TOKEN.

Why here and not in assets.py or resources.py?
  __init__.py is the first module Dagster imports. Calling load_dotenv() here
  guarantees the env vars are set before any EnvVar() resolver runs, before any
  DbtCliResource reads the profiles_dir, and before any asset function executes.

In production you would NOT use a .env file — you'd inject real environment
variables via Dagster Cloud, Kubernetes secrets, or your CI/CD system. The
load_dotenv() call is harmless in production because it's a no-op if the .env
file doesn't exist (override=False by default).
"""

from pathlib import Path

# Load .env FIRST — before any Dagster code that reads EnvVar().
# dotenv_path points to dagster/.env relative to this file's location.
from dotenv import load_dotenv

_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)
# override=False: if an env var is already set in the shell (e.g. in CI),
# the .env file value is NOT used. This lets CI/CD inject real secrets
# without the .env file overriding them.

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_project.assets import tmdb_raw_movies, dbt_netflix_assets, DBT_PROJECT_DIR
from dagster_project.resources import snowflake_resource
from dagster_project.schedules import daily_schedule, daily_pipeline_job

# ── DbtCliResource ────────────────────────────────────────────────────────────
# DbtCliResource tells dagster-dbt where to find the dbt executable and project.
#
# project_dir: the root of the netflix dbt project (contains dbt_project.yml).
#   Used as the working directory when spawning `dbt build` subprocesses.
#
# profiles_dir: NOT set here because the netflix profile lives in ~/.dbt/profiles.yml
#   (the standard dbt default location). DbtCliResource defaults to ~/.dbt when
#   profiles_dir is omitted, so dbt will find the 'netflix' profile automatically.
#   If you ever move profiles.yml into the netflix/ project directory, set:
#       profiles_dir=str(DBT_PROJECT_DIR)
#
# global_config_flags: list of dbt global flags applied to every CLI invocation.
#   "--no-use-colors" makes dbt output cleaner in Dagster's log viewer, which
#   doesn't render ANSI color codes in the same way a terminal does.
# dbt_executable: explicitly point to the dbt binary inside the .venv.
# This is important because the system PATH may contain a different dbt binary
# (e.g. the Python 3.14 system dbt on this machine) that is incompatible with
# the Python 3.11 venv. Pointing to the venv dbt ensures consistency.
_VENV_DBT = Path(__file__).parent.parent / ".venv" / "bin" / "dbt"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    # profiles_dir omitted → defaults to ~/.dbt where the 'netflix' profile lives
    # Use venv dbt to avoid picking up the system dbt (wrong Python version)
    dbt_executable=str(_VENV_DBT) if _VENV_DBT.exists() else "dbt",
    global_config_flags=["--no-use-colors"],
)

# ── Definitions: the central Dagster registry ─────────────────────────────────
# Definitions.merge() is not used here because we have a flat asset list.
# All assets, resources, schedules, and jobs are combined in one Definitions object.
#
# assets:
#   - tmdb_raw_movies  : ingestion asset (raw TMDB data → Snowflake)
#   - dbt_netflix_assets: all dbt models as assets (staging → intermediate → marts)
#
# resources:
#   "snowflake": the SnowflakeResource — any asset that declares
#                `snowflake: SnowflakeResource` in its signature receives this.
#   "dbt"      : the DbtCliResource — dbt_netflix_assets receives this.
#
# schedules:
#   daily_schedule: fires daily_pipeline_job at 06:00 UTC.
#
# jobs:
#   daily_pipeline_job: materialises all assets. Referenced by daily_schedule.
#   Also listed here so it appears in the Dagster UI's Jobs tab independently
#   of the schedule — useful for manual runs.
defs = Definitions(
    assets=[tmdb_raw_movies, dbt_netflix_assets],
    resources={
        # The key "snowflake" matches the parameter name in tmdb_raw_movies:
        #   def tmdb_raw_movies(context, snowflake: SnowflakeResource)
        # Dagster injects resources by matching the parameter name to the key here.
        "snowflake": snowflake_resource,
        # The key "dbt" matches the parameter name in dbt_netflix_assets:
        #   def dbt_netflix_assets(context, dbt: DbtCliResource)
        "dbt": dbt_resource,
    },
    schedules=[daily_schedule],
    jobs=[daily_pipeline_job],
)
