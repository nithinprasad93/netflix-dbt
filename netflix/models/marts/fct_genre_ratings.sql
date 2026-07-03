With int_genres_exploded AS (
    SELECT * FROM {{ ref('int_genres_exploded') }}
),
genre_mapping AS (
    SELECT * FROM {{ ref('genre_mapping') }}
),
int_ratings_summary AS (
    SELECT * FROM {{ ref('int_ratings_summary') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['g.movie_id', 'g.genre']) }} as genre_rating_sk,
    g.movie_id,
    g.genre,
    gm.display_genre as display_genre,
    r.avg_rating,
    r.rating_count
FROM 
    int_genres_exploded g
LEFT JOIN 
    genre_mapping gm 
ON 
    g.genre = gm.raw_genre
LEFT JOIN 
    int_ratings_summary r
ON 
    g.movie_id = r.movie_id
