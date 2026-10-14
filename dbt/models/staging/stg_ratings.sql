with raw_ratings as (
    select * from {{ source('movielens', 'raw_ratings') }}
)

select
    userId as user_id,
    movieId as movie_id,
    rating,
    -- to_timestamp(from_unixtime(timestamp)) as rating_timestamp
        {{ unix_to_ts('timestamp') }} as rating_timestamp

from raw_ratings