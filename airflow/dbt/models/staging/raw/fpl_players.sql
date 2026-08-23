{{
config(
    materialized="view"
)
}}

select
    id,
    season,
    opta_id,
    fpl_seasonal_id,
    name,
    position
from {{ source('raw', 'fpl_players') }}