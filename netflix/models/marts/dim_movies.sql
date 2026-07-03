WITH int_movies_with_links AS (
    SELECT * FROM  {{ ref('int_movies_with_links') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['movie_id']) }} as movie_sk,
    movie_id,
    title,
    release_year,
    genres,
    imdb_id,
    tmdb_id
FROM int_movies_with_links