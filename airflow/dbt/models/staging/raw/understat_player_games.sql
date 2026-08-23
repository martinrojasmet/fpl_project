{{
config(
    materialized="view"
)
}}

select
    id,
    name,
    understat_game_id,
    team,
    minutes_played,
    shots,
    goals,
    assists,
    expected_goals,
    expected_assists,
    key_passes
from {{ source('raw', 'understat_player_games') }}