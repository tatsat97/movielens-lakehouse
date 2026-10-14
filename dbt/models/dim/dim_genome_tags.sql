with stg_genome_tags as (
    select * from {{ ref('stg_genome_tags') }}
)
select
    tag_id,
    initcap(trim(tag)) as tag_name
from stg_genome_tags