SELECT
    display_genre,
    SUM(avg_rating * rating_count) / SUM(rating_count) AS weighted_avg_rating,
    SUM(rating_count) AS total_ratings
FROM {{ ref('fct_genre_ratings') }}
GROUP BY display_genre
ORDER BY weighted_avg_rating DESC