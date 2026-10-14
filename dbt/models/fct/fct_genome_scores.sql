with src_genome_scores as (
    select * from {{ ref('stg_genome_scores') }}
)

select
    movie_id,
    tag_id,
    round(relevance, 4) as relevance_score
from src_genome_scores
where relevance > 0