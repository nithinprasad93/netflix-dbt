{{ config(materialized='ephemeral') }}

WITH stg_tags AS (
    SELECT * FROM {{ ref('stg_tags') }}
)

SELECT
    user_id,
    movie_id,
    tag,
    tag_timestamp
FROM stg_tags
