with fpl_upcoming_games as (
    select
        fpl_game_id,
        season,
        gameweek,
        datetime,
        home_team_fpl_id,
        away_team_fpl_id,
        home_team_difficulty,
        away_team_difficulty
    from {{ source('raw', 'fpl_upcoming_games') }}
    where datetime >= current_timestamp
)
, unpivoted_fpl_upcoming_games as (
    select
        fpl_game_id,
        season,
        gameweek,
        datetime,
        home_team_fpl_id as team_fpl_id,
        away_team_fpl_id as opponent_team_fpl_id,
        home_team_difficulty as team_difficulty,
        away_team_difficulty as opponent_team_difficulty,
        true as is_home
    from fpl_upcoming_games

    union

    select
        fpl_game_id,
        season,
        gameweek,
        datetime,
        away_team_fpl_id as team_fpl_id,
        home_team_fpl_id as opponent_team_fpl_id,
        away_team_difficulty as team_difficulty,
        home_team_difficulty as opponent_team_difficulty,
        false as is_home
    from {{ source('raw', 'fpl_upcoming_games') }}
)
, fpl_upcoming_games_rn as (
    select *,
        row_number() over (
            partition by team_fpl_id
            order by datetime desc
        ) as rn
    from unpivoted_fpl_upcoming_games
)
, next_games_x_team_fpl as (
    select
        fpl_game_id,
        season,
        gameweek,
        datetime,
        team_fpl_id,
        opponent_team_fpl_id,
        is_home
    from fpl_upcoming_games_rn
    where rn in (1, 2, 3)
)
, next_games_x_team as (
    select
        g.fpl_game_id,
        g.season,
        g.gameweek,
        g.datetime,
        t.team_id,
        t2.team_id as opponent_team_id,
        is_home
    from next_games_x_team_fpl g
    inner join {{ ref('fpl_team_mappings') }} t
        on g.team_fpl_id = t.fpl_team_id and g.season = t.season
    inner join {{ ref('fpl_team_mappings') }} t2
        on g.opponent_team_fpl_id = t2.fpl_team_id and g.season = t2.season
)

select *
from next_games_x_team