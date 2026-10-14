with ratings as (
    select * from {{ ref('fct_ratings') }}
),

movies as (
    select * from {{ ref('dim_movies') }}
)

select
    movies.movie_title,
    count(*) as rating_count,
    round(avg(ratings.rating), 2) as avg_rating
from ratings
join movies on ratings.movie_id = movies.movie_id
group by movies.movie_title
having count(*) >= 100
order by avg_rating desc
limit 20