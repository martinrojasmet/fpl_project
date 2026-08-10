{{
config(
    materialized="view"
)
}}

select
    id,
    season,
    gameweek,
    fpl_game_id,
    fpl_datetime,
    home_fpl_team_id,
    away_fpl_team_id,
    home_goals,
    away_goals
from {{ source('raw', 'fpl_games') }}