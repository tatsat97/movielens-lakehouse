with stg_movies as (
    select * from {{ ref('stg_movies') }}
)

select
    movie_id,
    initcap(trim(title)) as movie_title,
    split(genres, '\\|') as genre_array,
    genres
from stg_movies