{{ config(materialized='ephemeral') }}

WITH stg_genome_tags AS (
    SELECT * FROM {{ ref('stg_genome_tags') }}
)

SELECT
    tag_id,
    tag
FROM stg_genome_tags
