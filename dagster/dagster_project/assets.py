"""
assets.py
=========
Dagster Software-Defined Asset (SDA) definitions for the Netflix/MovieLens pipeline.

This file contains two categories of assets:

  1. tmdb_raw_movies  — A hand-written Python asset that calls the TMDB REST API
                        and upserts results into Snowflake raw.raw_tmdb_movies.
                        This mirrors the extract_tmdb + load_to_snowflake tasks
                        from the Airflow DAG (airflow/dags/tmdb_pipeline.py) but
                        expressed as a single idempotent Dagster asset.

  2. dbt_netflix_assets — Auto-generated from the dbt project manifest using
                          @dbt_assets. Every dbt model, seed, test, and snapshot
                          becomes a Dagster asset node. Dependencies between dbt
                          models are reflected as Dagster asset dependencies
                          automatically — no manual wiring required.

Why SDAs over Ops/Jobs?
  Software-Defined Assets make data lineage first-class. Dagster tracks *what*
  data was produced (the asset) separately from *how* it was produced (the code).
  This enables:
    - Asset-level retry (re-materialise only failed assets, not the whole job)
    - Staleness detection (Dagster knows when an upstream changed)
    - Auto-materialisation policies
    - A lineage graph in the UI showing tmdb_raw_movies → dbt staging → dbt marts
"""

import os
import json
import requests

from pathlib import Path

from dagster import asset, AssetExecutionContext, Output
from dagster_snowflake import SnowflakeResource
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# ── dbt project path ──────────────────────────────────────────────────────────
# The netflix/ dbt project sits one directory above this dagster/ folder.
# Using Path(__file__) makes the path relative to this file, not to wherever
# `dagster dev` is invoked from — important for reproducibility.
#
# Path resolution:
#   __file__  = .../dagster/dagster_project/assets.py
#   .parent   = .../dagster/dagster_project/
#   .parent   = .../dagster/
#   / "netflix" → .../netflix/    (the dbt project root)
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "netflix"

# ── DbtProject: tells dagster-dbt where the dbt project lives ─────────────────
# DbtProject is the newer (dagster-dbt >= 0.22) way to register a dbt project.
# It reads dbt_project.yml from project_dir and locates the manifest at
# project_dir/target/manifest.json.
#
# IMPORTANT: manifest.json must exist before `dagster dev` starts.
# Run `dbt compile` or `dbt build` in the netflix/ directory first.
# The CI workflow (.github/workflows/dbt_ci.yml) already does this on every push.
# For local dev: cd netflix && dbt compile
#
# profiles_dir: The netflix profile lives in ~/.dbt/profiles.yml (the standard
# dbt default). We pass it explicitly here so that prepare_if_dev() knows where
# to find the profile when it optionally runs `dbt parse` to refresh the manifest.
# Without this, DbtProject would look in project_dir for profiles.yml and fail.
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    # Point to ~/.dbt where profiles.yml contains the 'netflix' profile.
    profiles_dir=Path.home() / ".dbt",
)

# ── Manifest validation ────────────────────────────────────────────────────────
# We do NOT call dbt_project.prepare_if_dev() here. That method runs `dbt parse`
# as a subprocess to regenerate manifest.json, but it uses whatever `dbt` binary
# is on PATH. On this machine the PATH dbt is the Python 3.14 version which is
# incompatible with our Python 3.11 venv and fails with an import error.
#
# Instead, we rely on the pre-existing manifest.json that was generated when
# the dbt project was last built (dbt build / dbt compile in netflix/).
# If you change dbt models, re-run `dbt compile` in the netflix/ directory with
# the venv dbt:
#   cd netflix && ../dagster/.venv/bin/dbt compile
#
# We validate the manifest exists at startup to give a clear error message.
_manifest_path = dbt_project.manifest_path
if not _manifest_path.exists():
    raise FileNotFoundError(
        f"dbt manifest.json not found at {_manifest_path}. "
        "Run: cd netflix && ../dagster/.venv/bin/dbt compile"
    )


# ── Asset 1: tmdb_raw_movies ──────────────────────────────────────────────────
@asset(
    # group_name groups assets visually in the Dagster UI asset graph.
    # "ingestion" signals this is a raw data landing zone asset.
    group_name="ingestion",
    # description shows in the Dagster UI asset catalog.
    description=(
        "Fetches enriched movie metadata from the TMDB API for every movie in "
        "marts.dim_movies that has a tmdb_id, then upserts the results into "
        "raw.raw_tmdb_movies using a Snowflake MERGE statement."
    ),
    # compute_kind shows the technology badge in the Dagster UI asset graph.
    compute_kind="python",
)
def tmdb_raw_movies(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output:
    """
    TMDB API ingestion asset.

    Execution flow:
      1. Query marts.dim_movies for distinct tmdb_ids (NULL ids skipped).
      2. Call TMDB v3 /movie/{id} for each id using Bearer token auth.
      3. Create raw.raw_tmdb_movies if it doesn't exist (self-bootstrapping DDL).
      4. MERGE each movie into raw.raw_tmdb_movies (idempotent upsert).

    This is a direct port of the Airflow extract_tmdb + load_to_snowflake tasks.
    The key difference: in Dagster, both steps live in one asset function because
    the intermediate list of movies is not a durable data asset — it's transient
    in-memory state. Splitting it into two assets would add coordination overhead
    with no lineage benefit.

    The `snowflake` parameter is dependency-injected by Dagster from the resource
    registered in Definitions (see __init__.py). The asset never constructs the
    connection itself — this keeps the asset testable and the config centralised.
    """

    # ── Step 1: Read tmdb_ids from Snowflake ──────────────────────────────────
    # snowflake.get_connection() returns a snowflake-connector-python Connection.
    # Using a context manager ensures the connection is closed even if the asset
    # raises an exception partway through.
    context.log.info("Fetching tmdb_ids from marts.dim_movies...")

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # LIMIT 100 keeps the TMDB API call count manageable in development.
        # Remove the LIMIT (or make it a config parameter) for full production runs.
        # WHERE tmdb_id IS NOT NULL: the MovieLens dataset contains many movies
        # without TMDB ids (obscure or pre-TMDB films) — skip them gracefully.
        cursor.execute(
            "SELECT DISTINCT tmdb_id FROM marts.dim_movies "
            "WHERE tmdb_id IS NOT NULL LIMIT 100"
        )
        tmdb_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()

    context.log.info(f"Found {len(tmdb_ids)} tmdb_ids to fetch")

    # ── Step 2: Call the TMDB API ─────────────────────────────────────────────
    # TMDB v3 uses Bearer token (JWT) auth, not the legacy API key.
    # The token is read from TMDB_BEARER_TOKEN env var loaded from .env.
    # Never hardcode the token — it grants read access to your entire TMDB account.
    bearer_token = os.environ.get("TMDB_BEARER_TOKEN")
    if not bearer_token:
        raise ValueError("TMDB_BEARER_TOKEN environment variable is not set")

    headers = {"Authorization": f"Bearer {bearer_token}"}
    movies = []

    for tmdb_id in tmdb_ids:
        response = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}",
            headers=headers,
            # 10-second timeout prevents a single slow TMDB response from hanging
            # the entire asset run indefinitely.
            timeout=10,
        )
        if response.status_code == 200:
            movies.append(response.json())
        elif response.status_code == 404:
            # 404 is expected for movies removed from TMDB — log and skip.
            context.log.warning(f"tmdb_id={tmdb_id} not found on TMDB (404), skipping")
        else:
            # Non-200, non-404 responses are unexpected — log but don't abort the
            # whole run. A single bad response shouldn't prevent the rest from loading.
            context.log.warning(
                f"tmdb_id={tmdb_id} returned status {response.status_code}, skipping"
            )

    context.log.info(f"Successfully fetched {len(movies)} movies from TMDB")

    if not movies:
        context.log.info("No movies to load — exiting early")
        return Output(value=0, metadata={"movies_loaded": 0})

    # ── Step 3: Create raw.raw_tmdb_movies if it doesn't exist ───────────────
    # Self-bootstrapping DDL means the pipeline works on a fresh Snowflake account
    # without manual setup. The IF NOT EXISTS guard makes this idempotent.
    #
    # raw_json VARIANT stores the full TMDB API response. This follows the
    # "store everything raw, extract in dbt" pattern — if TMDB adds a new field
    # tomorrow, it's already in the VARIANT column, and a dbt model can expose it
    # without changing this ingestion code.
    with snowflake.get_connection() as conn:
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

        # ── Step 4: MERGE (upsert) each movie ────────────────────────────────
        # MERGE is used instead of INSERT or TRUNCATE+INSERT to make this asset
        # idempotent: running it twice with the same TMDB data produces the same
        # table state. This is critical for:
        #   - Safe retries: if the asset fails at movie #50, rerunning from the
        #     start won't duplicate movies #1-49.
        #   - Incremental updates: TMDB sometimes corrects metadata (wrong runtime,
        #     revised budget). WHEN MATCHED UPDATE ensures corrections propagate.
        #
        # %s placeholders are parameterised by snowflake-connector-python, which
        # prevents SQL injection and handles Python→Snowflake type coercion.
        loaded_count = 0
        for movie in movies:
            cursor.execute(
                """
                MERGE INTO raw.raw_tmdb_movies AS target
                USING (SELECT %s::NUMBER AS tmdb_id) AS source
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
                    (tmdb_id, title, overview, runtime, popularity, budget,
                     revenue, release_date, poster_path, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
                """,
                (
                    # MATCHED branch (10 params after the ON clause)
                    movie.get("id"),          # source.tmdb_id for the JOIN
                    movie.get("title"),
                    movie.get("overview"),
                    movie.get("runtime"),
                    movie.get("popularity"),
                    movie.get("budget"),
                    movie.get("revenue"),
                    movie.get("release_date"),
                    movie.get("poster_path"),
                    json.dumps(movie),         # PARSE_JSON() on the UPDATE branch
                    # NOT MATCHED branch (10 more params for the INSERT VALUES)
                    movie.get("id"),
                    movie.get("title"),
                    movie.get("overview"),
                    movie.get("runtime"),
                    movie.get("popularity"),
                    movie.get("budget"),
                    movie.get("revenue"),
                    movie.get("release_date"),
                    movie.get("poster_path"),
                    json.dumps(movie),         # PARSE_JSON() on the INSERT branch
                ),
            )
            loaded_count += 1

        cursor.close()

    context.log.info(f"Upserted {loaded_count} movies into raw.raw_tmdb_movies")

    # Output wraps the return value and attaches metadata that shows in the
    # Dagster UI run page and asset catalog — useful for monitoring at a glance.
    return Output(
        value=loaded_count,
        metadata={
            "movies_loaded": loaded_count,
            "tmdb_ids_queried": len(tmdb_ids),
        },
    )


# ── Asset 2: dbt_netflix_assets ───────────────────────────────────────────────
# @dbt_assets reads the dbt manifest and generates one Dagster asset per dbt node.
#
# How dagster-dbt resolves dependencies:
#   - Each dbt model becomes an asset keyed by its model name.
#   - If model B has `ref('model_A')`, Dagster treats model_A as an upstream
#     asset of model_B. The full dbt DAG is reflected as Dagster asset dependencies.
#   - Sources (defined in sources.yml) become "external" assets with no upstream —
#     Dagster knows they come from outside the dbt project.
#
# What @dbt_assets does NOT do:
#   - It does not run dbt compile. The manifest must exist before Dagster starts.
#   - It does not manage Snowflake credentials — that's DbtCliResource's job.
#
# The `dbt` parameter is dependency-injected from the DbtCliResource registered
# in Definitions (see __init__.py). The asset function calls dbt CLI subcommands
# (build, test, run) via the resource, which handles subprocess management,
# log streaming to the Dagster run log, and error propagation.
@dbt_assets(
    manifest=dbt_project.manifest_path,
    # project is optional but recommended — it enables dbt project-level
    # configuration (e.g. selecting models by tag or path) from the Dagster UI.
    project=dbt_project,
)
def dbt_netflix_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    All dbt models in the netflix project as Dagster assets.

    dagster-dbt invokes `dbt build` which runs models, seeds, snapshots, and
    tests in dependency order. Each dbt node's success/failure is reported
    individually to the Dagster run log.

    dbt build vs dbt run:
      `dbt build` is preferred because it also runs tests after each model
      (fail-fast on bad data) and handles seeds and snapshots in the same command.
      `dbt run` only runs models.
    """
    # dbt.cli("build") launches `dbt build` as a subprocess.
    # stream() forwards dbt's stdout/stderr to the Dagster run log in real time,
    # so you can see individual model results as they complete in the UI.
    # fetch_dagster_events() parses dbt's structured log output and converts
    # each model result into a Dagster AssetMaterialization event, which updates
    # the asset catalog.
    yield from dbt.cli(["build"], context=context).stream()
