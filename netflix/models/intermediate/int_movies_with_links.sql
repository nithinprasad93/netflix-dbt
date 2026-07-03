WITH stg_movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
),

stg_links AS (
    SELECT * FROM {{ ref('stg_links') }}
)

SELECT
    mov.movie_id,
    mov.title,
    REGEXP_SUBSTR(mov.title, '\\(([0-9]{4})\\)', 1, 1, 'e') AS release_year,
    mov.genres,
    lks.imdb_id,
    lks.tmdb_id
FROM stg_movies mov
LEFT JOIN stg_links lks
    ON mov.movie_id = lks.movie_id