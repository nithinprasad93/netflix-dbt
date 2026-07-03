{% snapshot snap_dim_movies %}

{{
    config(
        target_schema='snapshots',
        unique_key='movie_sk',
        strategy='check',
        check_cols=['genres', 'release_year', 'imdb_id', 'tmdb_id']
    )
}}

SELECT * FROM  {{ ref('dim_movies') }}

{% endsnapshot %}