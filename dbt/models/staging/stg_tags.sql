with raw_tags as (
    select * from {{ source('movielens', 'raw_tags') }}
)

select
    userId as user_id,
    movieId as movie_id,
    tag,
    -- to_timestamp(from_unixtime(timestamp)) as tag_timestamp
        {{ unix_to_ts('timestamp') }} as tag_timestamp

from raw_tags