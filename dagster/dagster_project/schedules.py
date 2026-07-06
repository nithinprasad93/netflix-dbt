"""
schedules.py
============
Dagster schedule definitions for the Netflix / MovieLens pipeline.

Why a ScheduleDefinition that targets a job vs. a plain @schedule decorator?
  ScheduleDefinition with job= is the simpler approach when the entire asset
  graph should run on a single cadence. It internally creates a job that
  materialises all assets in the repository and fires it on the cron expression.

  The @schedule decorator is more powerful (you can vary run config per tick)
  but unnecessary here since every daily run does the same thing.

Cron expression "0 6 * * *":
  Field order: minute hour day-of-month month day-of-week
  "0 6 * * *" = 06:00 UTC every day
  Chosen as 06:00 UTC (roughly 1–2 AM US Eastern) so the pipeline completes
  before business hours and after overnight batch loads have finished.

  Change to "@daily" (= "0 0 * * *") for midnight UTC, or "0 2 * * *" for
  2 AM UTC depending on your data availability window.

execution_timezone:
  Always specify a timezone. Without it, Dagster uses UTC by default, but
  being explicit prevents confusion when the team is in a different timezone.
  "America/New_York" is shown as an example — change to match your SLA.
"""

from dagster import ScheduleDefinition, define_asset_job, AssetSelection

# ── Daily job: materialise every asset ───────────────────────────────────────
# AssetSelection.all() targets every asset registered in the Definitions object.
# This means the TMDB ingestion asset runs first (it has no upstream dependencies),
# then all dbt models run in their dependency order (managed by dagster-dbt).
#
# Why define the job here rather than in __init__.py?
#   Keeping schedules.py self-contained makes it easier to add more jobs later
#   (e.g. an hourly TMDB-only job that skips dbt, or a weekly full-refresh job).
daily_pipeline_job = define_asset_job(
    name="daily_pipeline_job",
    # AssetSelection.all() means: materialise every asset that Dagster knows about.
    # As new dbt models are added to the netflix project, they are automatically
    # included in this job — no schedule changes needed.
    selection=AssetSelection.all(),
)

# ── Schedule definition ───────────────────────────────────────────────────────
# Ties the daily_pipeline_job to a cron expression.
# Dagster's scheduler daemon must be running for the schedule to fire automatically
# (`dagster dev` starts the daemon automatically in development).
daily_schedule = ScheduleDefinition(
    name="daily_netflix_pipeline",
    # Cron: 06:00 UTC every day.
    # Adjust to match your data availability window.
    cron_schedule="0 6 * * *",
    job=daily_pipeline_job,
    # execution_timezone ensures cron is interpreted in a known timezone.
    # Dagster defaults to UTC but being explicit prevents daylight-saving surprises.
    execution_timezone="UTC",
)
