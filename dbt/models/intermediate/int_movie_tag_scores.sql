with scores as (
    select * from {{ ref('fct_genome_scores') }}
),

tags as (
    select * from {{ ref('dim_genome_tags') }}
)

select
    scores.movie_id,
    tags.tag_name,
    scores.relevance_score
from scores
join tags on scores.tag_id = tags.tag_id
where scores.relevance_score > 0.5