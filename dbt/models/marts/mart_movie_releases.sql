
with ratings as (
    select * from {{ ref('fct_ratings') }}
),

movies as (
    select * from {{ ref('dim_movies') }}
),

release_dates as (
    select * from {{ ref('seed_movie_release_date') }}
)

select
    ratings.user_id,
    ratings.movie_id,
    movies.movie_title,
    ratings.rating,
    ratings.rating_timestamp,
    release_dates.release_date,
    case
        when release_dates.release_date is null then 'unknown'
        else 'known'
    end as release_info_available
from ratings
left join movies on ratings.movie_id = movies.movie_id
left join release_dates on ratings.movie_id = release_dates.movie_id