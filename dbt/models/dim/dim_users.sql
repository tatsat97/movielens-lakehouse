

with ratings_users as (
    select distinct user_id from {{ ref('stg_ratings') }}
),

tags_users as (
    select distinct user_id from {{ ref('stg_tags') }}
)

select user_id
from ratings_users
union
select user_id
from tags_users-- ci test
