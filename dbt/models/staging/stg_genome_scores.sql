with raw_genome_scores as (
    select * from {{ source('movielens', 'raw_genome_scores') }}
)

select
    movieId as movie_id,
    tagId as tag_id,
    relevance
from raw_genome_scores