{{ config(materialized='incremental', unique_key=['user_id', 'movie_id']) }}

With int_ratings AS (
    SELECT * FROM {{ ref('int_ratings') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id']) }} as rating_sk,
    user_id,
    movie_id,
    rating,
    rating_timestamp
FROM int_ratings
{% if is_incremental() %}
    WHERE rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}
