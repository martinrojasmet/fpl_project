{{
config(
    materialized="view"
)
}}

select
    team_id,
    season,
    fpl_team_id
from {{ ref('team_mappings') }}