{{ config(
    unique_key=['team_id', 'season'],
    incremental_strategy='merge'
) }}

with seed_team_mappings as (
    {% if not is_incremental() %}
    select
        id,
        season,
        team_id,
        fpl_team_id,
        fpl_name,
        understat_name
    from {{ ref('fpl_db_master_team_mappings') }}
    {% else %}
    select
        id,
        season,
        team_id,
        fpl_team_id,
        fpl_name,
        understat_name
    from {{ ref('fpl_db_master_team_mappings') }}
    where false
    {% endif %}

)
, database_team_mappings as (
    select
        id,
        season,
        team_id,
        fpl_team_id,
        fpl_name,
        understat_name
    from {{ source('master', 'team_mappings') }}
)
, all_team_mappings as (
    select
        coalesce(i.id, d.id) as id,
        coalesce(d.season, i.season) as season,
        coalesce(i.team_id, d.team_id) as team_id,
        coalesce(d.fpl_team_id, i.fpl_team_id) as fpl_team_id,
        coalesce(d.fpl_name, i.fpl_name) as fpl_name,
        coalesce(d.understat_name, i.understat_name) as understat_name
    from database_team_mappings d
    full outer join seed_team_mappings i
        on i.season = d.season 
       and i.team_id = d.team_id
)

select *
from all_team_mappings