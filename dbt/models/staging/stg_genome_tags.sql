with raw_genome_tags as (
    select * from {{ source('movielens', 'raw_genome_tags') }}
)

select
    tagId as tag_id,
    tag
from raw_genome_tags