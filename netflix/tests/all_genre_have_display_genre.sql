SELECT *
FROM {{ ref('fct_genre_ratings') }}
WHERE display_genre IS NULL