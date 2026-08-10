{{ config(
    unique_key=['season', 'home_team_id', 'away_team_id'],
    incremental_strategy='merge'
) }}

with initial_games as (
    {% if not is_incremental() %}
    select
        id,
        season,
        gameweek,
        datetime,
        home_team_id,
        away_team_id,
        result
    from {{ ref('fpl_db_core_games') }}
    {% else %}
    select
        id,
        season,
        gameweek,
        datetime,
        home_team_id,
        away_team_id,
        result
    from {{ this }}
    {% endif %}
)
, raw_fpl_games as (
    select
        f.season as season,
        f.gameweek,
        f.fpl_datetime as datetime,
        tm_home.team_id as home_team_id,
        tm_away.team_id as away_team_id,
        case
            when f.home_goals > f.away_goals then 'home_win'
            when f.home_goals < f.away_goals then 'away_win'
            else 'tie'
        end as result
    from {{ ref('fpl_games') }} f
    inner join {{ ref('fpl_team_mappings') }} as tm_home 
        on f.home_fpl_team_id = tm_home.fpl_team_id and f.season = tm_home.season
    inner join {{ ref('fpl_team_mappings') }} as tm_away 
        on f.away_fpl_team_id = tm_away.fpl_team_id and f.season = tm_away.season
)
, raw_understat_games as (
    select
        u.season as season,
        u.date as datetime,
        tm_home.team_id as home_team_id,
        tm_away.team_id as away_team_id
    from {{ ref('understat_games') }} u
    inner join {{ ref('understat_team_mappings') }}
        tm_home on u.home = tm_home.name
        and u.season = tm_home.season
    inner join {{ ref('understat_team_mappings') }} tm_away on u.away = tm_away.name
        and u.season = tm_away.season
)
, pipeline_games as (
    select
        gen_random_uuid() as id,
        coalesce(f.season, u.season) as season,
        f.gameweek,
        coalesce(f.datetime, u.datetime) as datetime,
        coalesce(f.home_team_id, u.home_team_id) as home_team_id,
        coalesce(f.away_team_id, u.away_team_id) as away_team_id,
        f.result
    from raw_fpl_games f
    left join raw_understat_games u 
        on f.season = u.season 
       and f.home_team_id = u.home_team_id 
       and f.away_team_id = u.away_team_id
)
, all_games as (
    select
        coalesce(i.id, p.id) as id,
        coalesce(p.season, i.season) as season,
        coalesce(p.gameweek, i.gameweek) as gameweek,
        coalesce(p.datetime, i.datetime) as datetime,
        coalesce(i.home_team_id, p.home_team_id) as home_team_id,
        coalesce(i.away_team_id, p.away_team_id) as away_team_id,
        coalesce(p.result, i.result) as result
    from pipeline_games p
    {% if not is_incremental() %}
    full outer join initial_games i
    {% else %}
    left join initial_games i
    {% endif %}
        on i.season = p.season 
       and i.home_team_id = p.home_team_id 
       and i.away_team_id = p.away_team_id
)

select *
from all_games