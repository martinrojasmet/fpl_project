with active_players as (
    select distinct
        id
    from {{ ref('players') }}
    where is_active
)
, current_team_per_player as (
    select
        pt.player_id,
        pt.team_id
    from active_players ap
    join {{ ref('player_teams')}} pt
        on ap.id = pt.player_id
    where pt.end_date is null
)

select
    player_id,
    team_id
from current_team_per_player