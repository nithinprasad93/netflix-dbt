WITH int_tags AS (
    SELECT * FROM  {{ ref('int_tags') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['tag_id']) }} as tag_sk,
    tag_id,
    tag
FROM int_tags