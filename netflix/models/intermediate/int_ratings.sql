{{ config(materialized='ephemeral') }}

WITH stg_ratings AS (
    SELECT * FROM {{ ref('stg_ratings') }}
)

SELECT
    user_id,
    movie_id,
    rating,
    rating_timestamp
FROM stg_ratings
