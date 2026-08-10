{{ config(
    unique_key=['season', 'player_id'],
    incremental_strategy='merge'
) }}

with initial_ps as (
    {% if not is_incremental() %}
    select
        id,
        season,
        player_id,
        position
    from {{ ref('fpl_db_core_player_seasons') }}
    {% else %}
    select
        id,
        season,
        player_id,
        position
    from {{ this }}
    {% endif %}
)
, pipeline_ps as (
    select
        gen_random_uuid() as id,
        p.season,
        p.player_id,
        max(p.position) as position
    from {{ ref('player_mappings') }} p
    where p.player_id is not null
      and p.season is not null
    group by p.season, p.player_id
)
, all_ps as (
    select
        coalesce(s.id, p.id) as id,
        coalesce(p.season, s.season) as season,
        coalesce(p.player_id, s.player_id) as player_id,
        coalesce(p.position, s.position) as position
    from pipeline_ps p
    {% if not is_incremental() %}
    full outer join initial_ps s
    {% else %}
    left join initial_ps s
    {% endif %}
        on s.season = p.season 
       and s.player_id = p.player_id
)

select *
from all_ps