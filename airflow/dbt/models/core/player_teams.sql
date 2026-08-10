{{ config(
    unique_key=['player_id', 'team_id', 'start_date'],
    incremental_strategy='merge'
) }}

with initial_pt as (
    {% if not is_incremental() %}
    select
        id,
        player_id,
        team_id,
        start_date,
        end_date
    from {{ ref('fpl_db_core_player_teams') }}
    {% else %}
    select
        id,
        player_id,
        team_id,
        start_date,
        end_date
    from {{ this }}
    {% endif %}
)
, pipeline_raw_pt as (
    select
        pg.player_id,
        tm.team_id,
        g.datetime as game_date
    from {{ ref('player_games') }} pg
    inner join {{ ref('games') }} g 
        on pg.game_id = g.id
    inner join {{ ref('game_mappings') }} gm 
        on g.id = gm.game_id
    inner join {{ ref('player_mappings') }} pm 
        on pg.player_id = pm.player_id 
       and g.season = pm.season
    inner join {{ ref('fpl_player_games') }} fpg 
        on gm.fpl_game_id = fpg.fpl_game_id 
       and g.season = fpg.season
       and (
           (fpg.opta_id = pm.opta_id)
           or (fpg.fpl_player_id = pm.fpl_seasonal_id)
           or (fpg.fpl_element = pg.fpl_element)
       )
    inner join {{ ref('fpl_team_mappings') }} tm 
        on fpg.fpl_team_id = tm.fpl_team_id 
       and g.season = tm.season
)
, seed_raw_pt as (
    select
        pg.player_id,
        s.team_id,
        g.datetime as game_date
    from {{ ref('player_games') }} pg
    inner join {{ ref('games') }} g 
        on pg.game_id = g.id
    inner join initial_pt s 
        on pg.player_id = s.player_id
       and g.datetime >= s.start_date
       and (g.datetime < s.end_date or s.end_date is null)
)
, pipeline_pt as (
    select
        coalesce(p.player_id, s.player_id) as player_id,
        coalesce(p.team_id, s.team_id) as team_id,
        coalesce(p.game_date, s.game_date) as game_date 
    from seed_raw_pt s
    full outer join pipeline_raw_pt p
        on p.player_id = s.player_id
       and p.game_date = s.game_date
)
, pipeline_stint_changes as (
    select
        player_id,
        team_id,
        game_date,
        case
            when lag(team_id) over (partition by player_id order by game_date asc) = team_id then 0
            else 1
        end as is_new_stint
    from pipeline_pt
)
, pipeline_stint_groups as (
    select
        player_id,
        team_id,
        game_date,
        sum(is_new_stint) over (partition by player_id order by game_date asc) as stint_id
    from pipeline_stint_changes
)
, pipeline_collapsed_stints as (
    select
        player_id,
        team_id,
        min(game_date) as start_date
    from pipeline_stint_groups
    group by
        player_id,
        stint_id,
        team_id
)
, combined_pt_start_dates as (
    select
        coalesce(s.id, gen_random_uuid()) as id,
        coalesce(c.player_id, s.player_id) as player_id,
        coalesce(s.team_id, c.team_id) as team_id,
        coalesce(c.start_date, s.start_date) as start_date
    from pipeline_collapsed_stints c
    left join initial_pt s
        on c.player_id = s.player_id
       and c.start_date = s.start_date
       and c.team_id = s.team_id
)

select 
    id,
    player_id,
    team_id,
    start_date,
    lead(start_date) over (
        partition by player_id 
        order by start_date asc
    ) as end_date
from combined_pt_start_dates