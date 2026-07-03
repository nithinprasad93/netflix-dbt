WITH stg_ratings AS (
    SELECT * FROM {{ ref('stg_ratings') }}
)
SELECT
    movie_id,
    AVG(rating) AS avg_rating,
    COUNT(rating) AS rating_count,
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating
FROM stg_ratings
GROUP BY movie_id