"""
resources.py
============
Dagster resource definitions for this pipeline.

Resources in Dagster are shared, configurable objects that assets and ops
depend on — analogous to dependency injection. Defining resources here (rather
than instantiating connections inside each asset function) means:

  1. Credentials live in one place — change the .env and every asset picks it up.
  2. Resources can be swapped for tests (e.g. replace the real Snowflake resource
     with a mock without changing any asset code).
  3. Dagster's UI shows resource configuration, making debugging easier.

Why EnvVar() instead of os.getenv()?
  EnvVar() is Dagster's lazy environment variable reader. It defers reading the
  environment until the resource is actually configured and used, rather than at
  import time. This is important because python-dotenv loads the .env file in
  __init__.py — if we used os.getenv() at module import time (before dotenv has
  run), the values would be None.

Why SnowflakeResource from dagster-snowflake?
  dagster-snowflake wraps snowflake-connector-python with:
    - Automatic connection pooling and cleanup
    - Dagster's IO manager protocol (optional)
    - execute_query() and fetch_results() helpers
    - Proper error propagation that shows in the Dagster UI run log
"""

from dagster_snowflake import SnowflakeResource
from dagster import EnvVar

# ── Snowflake resource ────────────────────────────────────────────────────────
# This resource is referenced by name in the Definitions object in __init__.py
# and injected into any asset that declares `snowflake: SnowflakeResource` in
# its function signature.
#
# All values are read from environment variables via EnvVar(), which python-dotenv
# populates from dagster/.env before Dagster starts (see __init__.py).
#
# schema="RAW" sets the default schema for the connection. The TMDB ingestion
# asset writes to raw.raw_tmdb_movies, so RAW is the right default. dbt models
# manage their own schema targeting via dbt_project.yml, not through this resource.
snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    schema="RAW",  # default schema; individual queries can qualify with schema.table
)
