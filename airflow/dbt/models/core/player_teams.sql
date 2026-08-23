{{ config(
    unique_key=['player_id', 'team_id', 'start_date'],
    incremental_strategy='merge'
) }}

with active_players as (
    select distinct
        id as player_id,
        true as is_active
    from {{ ref('players') }}
    where is_active
)
, initial_pt as (
    {% if not is_incremental() %}
    select
        player_id,
        team_id,
        start_date,
        end_date
    from {{ ref('fpl_db_core_player_teams') }}
    {% else %}
    select
        player_id,
        team_id,
        start_date,
        end_date
    from {{ this }}
    {% endif %}
)
, initial_pt_start as (
    select
        player_id,
        team_id,
        start_date as game_date
    from initial_pt
)
, fpl_pg_teams as (
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
, raw_fpl_pt as (
    select
        pm.player_id,
        tm.team_id,
        datetime as game_date
    from {{ ref('fpl_player_teams') }} pt
    join {{ ref('player_mappings') }} pm
        on pt.opta_id = pm.opta_id
        and pt.season = pm.season
    join {{ ref('fpl_team_mappings') }} tm
        on pt.fpl_team_id = tm.fpl_team_id
        and pt.season = tm.season
)
, fpl_pg_initial_pt as (
    select
        coalesce(s.player_id, p.player_id) as player_id,
        coalesce(s.team_id, p.team_id) as team_id,
        coalesce(s.game_date, p.game_date) as game_date 
    from initial_pt_start s
    full outer join fpl_pg_teams p
        on p.player_id = s.player_id
       and p.game_date = s.game_date
)
, pt_union as (
    select
        player_id,
        team_id,
        game_date
    from fpl_pg_initial_pt

    union

    select
        player_id,
        team_id,
        game_date
    from raw_fpl_pt
)
, pt_stint_changes as (
    select
        player_id,
        team_id,
        game_date,
        case
            when lag(team_id) over (partition by player_id order by game_date asc) = team_id then 0
            else 1
        end as is_new_stint
    from pt_union
)
, pt_stint_groups as (
    select
        player_id,
        team_id,
        game_date,
        sum(is_new_stint) over (partition by player_id order by game_date asc) as stint_id
    from pt_stint_changes
)
, pt_collapsed_stints as (
    select
        player_id,
        team_id,
        min(game_date) as start_date
    from pt_stint_groups
    group by
        player_id,
        stint_id,
        team_id
)
, pt_stints_with_lead as (
    select 
        gen_random_uuid() as id,
        c.player_id,
        c.team_id,
        c.start_date,
        lead(c.start_date) over (
            partition by c.player_id 
            order by c.start_date asc
        ) as next_start_date,
        ap.is_active
    from pt_collapsed_stints c
    left join active_players ap
        on c.player_id = ap.player_id
)

select 
    id,
    player_id,
    team_id,
    start_date,
    case
        when next_start_date is not null then next_start_date
        when not coalesce(is_active, false) then current_date
        else null
    end as end_date
from pt_stints_with_lead