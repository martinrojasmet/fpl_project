select
    id,
    run_id,
    season,
    datetime,
    opta_id,
    fpl_player_seasonal_id,
    name, 
    fpl_team_id
from {{ source('raw','fpl_player_teams') }}