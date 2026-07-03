SELECT f.movie_id
FROM {{ ref('fct_tag_applications') }} f
LEFT JOIN {{ ref('dim_movies') }} d
    ON f.movie_id = d.movie_id
WHERE d.movie_id IS NULL