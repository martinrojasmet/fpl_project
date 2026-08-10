{{ config(
    unique_key='id',
    incremental_strategy='merge'
) }}

with initial_players as (
    {% if not is_incremental() %}
    select
        id,
        name,
        is_active
    from {{ ref('fpl_db_core_players') }}
    {% else %}
    select
        id,
        name,
        is_active
    from {{ this }}
    {% endif %}
)
, active_flags as (
    select
        p.player_id as id,
        bool_or(p.season = {{ fpl_current_season() }}) as is_active
    from {{ ref('player_mappings') }} p
    group by 1
)
, lastest_season_per_player as (
    select
        player_id,
        max(season) as latest_season
    from {{ ref('player_mappings') }}
    group by player_id
)
, pipeline_players as (
    select
        p.player_id as id,
        trim(p.fpl_name) as name
    from {{ ref('player_mappings') }} p
    inner join lastest_season_per_player latest
        on p.player_id = latest.player_id 
       and p.season = latest.latest_season
)
, all_players as (
    select
        coalesce(i.id, p.id) as id,
        coalesce(i.name, p.name) as name
    from pipeline_players p
    {% if not is_incremental() %}
    full outer join initial_players i
    {% else %}
    left join initial_players i
    {% endif %}
        on i.id = p.id
)

select
    d.id,
    d.name,
    coalesce(a.is_active, false) as is_active
from all_players d
left join active_flags a
    on d.id = a.id