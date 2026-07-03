WITH raw_links AS (
  SELECT * FROM {{ source('netflix', 'raw_links') }}
)

SELECT
  movieId AS movie_id,
  imdbId AS imdb_id,
  tmdbId AS tmdb_id
FROM raw_links