{% snapshot snap_raw_movies %}

{{
    config(
        target_schema='snapshots',
        unique_key='movieId',
        strategy='check',
        check_cols=['genres']
    )
}}

SELECT * FROM  {{ source('netflix', 'raw_movies') }}

{% endsnapshot %}