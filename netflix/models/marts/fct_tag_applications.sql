{{ config(materialized='incremental', unique_key=['user_id', 'movie_id', 'tag']) }}

With int_tag_applications AS (
    SELECT * FROM {{ ref('int_tag_applications') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'tag']) }} as tag_application_sk,
    user_id,
    movie_id,
    tag,
    tag_timestamp
FROM int_tag_applications
{% if is_incremental() %}
    WHERE tag_timestamp > (SELECT MAX(tag_timestamp) FROM {{ this }})
{% endif %}
