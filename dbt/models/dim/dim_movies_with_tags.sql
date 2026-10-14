with movies as (
    select * from {{ ref('dim_movies') }}
),

tag_scores as (
    select * from {{ ref('int_movie_tag_scores') }}
)

select
    movies.movie_id,
    movies.movie_title,
    movies.genre_array,
    tag_scores.tag_name,
    tag_scores.relevance_score
from movies
left join tag_scores on movies.movie_id = tag_scores.movie_id