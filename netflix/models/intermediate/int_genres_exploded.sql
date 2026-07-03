WITH int_movies_with_links AS (
    SELECT * FROM {{ ref('int_movies_with_links') }}
)

SELECT
    m.movie_id,
    TRIM(g.value) AS genre
FROM int_movies_with_links m,
    LATERAL SPLIT_TO_TABLE(m.genres, '|') g