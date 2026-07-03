WITH stg_genome_scores AS (
    SELECT * FROM {{ ref('stg_genome_scores') }}
),

stg_genome_tags AS (
    SELECT * FROM {{ ref('stg_genome_tags') }}
),

ranked_scores AS (
    SELECT
        movie_id,
        tag_id,
        relevance,
        ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY relevance DESC) AS tag_rank
    FROM stg_genome_scores
),

top_scores AS (
    SELECT * FROM ranked_scores WHERE tag_rank <= 10
)

SELECT
    t.movie_id,
    t.tag_id,
    g.tag,
    t.relevance,
    t.tag_rank
FROM top_scores t
JOIN stg_genome_tags g ON t.tag_id = g.tag_id