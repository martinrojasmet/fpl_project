{{
config(
    materialized="view"
)
}}

select
    id,
    season,
    name
from {{ source('raw', 'fpl_teams') }}