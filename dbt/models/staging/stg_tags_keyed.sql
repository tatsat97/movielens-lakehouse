select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'tag']) }} as user_movie_tag_id,
    user_id,
    movie_id,
    tag,
    tag_timestamp
from {{ ref('stg_tags') }}