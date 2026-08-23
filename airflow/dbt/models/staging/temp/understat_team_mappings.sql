{{
config(
    materialized="view"
)
}}

select
    team_id,
    season,
    understat_name as name
from {{ ref('team_mappings') }}