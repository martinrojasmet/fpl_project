{{ config(
    unique_key=['player_id', 'season'],
    incremental_strategy='merge'
) }}

with seed_player_mappings as (
    {% if not is_incremental() %}
    select
        id,
        season,
        player_id,
        fpl_seasonal_id,
        opta_id,
        fpl_name,
        understat_name,
        position
    from {{ ref('fpl_db_master_player_mappings') }}
    {% else %}
    select
        id,
        season,
        player_id,
        fpl_seasonal_id,
        opta_id,
        fpl_name,
        understat_name,
        position
    from {{ ref('fpl_db_master_player_mappings') }}
    where false
    {% endif %}
)
, database_player_mappings as (
    select
        id,
        season,
        player_id,
        fpl_seasonal_id,
        opta_id,
        fpl_name,
        understat_name,
        position
    from {{ source('master', 'player_mappings') }}
)
, all_player_mappings as (
    select
        coalesce(s.id, d.id) as id,
        coalesce(d.season, s.season) as season,
        coalesce(d.player_id, s.player_id) as player_id,
        coalesce(d.fpl_seasonal_id, s.fpl_seasonal_id) as fpl_seasonal_id,
        coalesce(d.opta_id, s.opta_id) as opta_id,
        coalesce(d.fpl_name, s.fpl_name) as fpl_name,
        coalesce(d.understat_name, s.understat_name) as understat_name,
        coalesce(d.position, s.position) as position
    from database_player_mappings d
    full outer join seed_player_mappings s
        on s.player_id = d.player_id 
       and s.season = d.season
)

select *
from all_player_mappings