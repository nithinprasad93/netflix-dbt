SELECT
    movie_id,
    title,
    avg_rating,
    rating_count
FROM {{ ref('int_ratings_summary') }}
JOIN {{ ref('dim_movies') }} USING (movie_id)
WHERE rating_count >= 100
ORDER BY avg_rating DESC
LIMIT 10