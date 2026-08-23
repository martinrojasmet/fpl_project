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