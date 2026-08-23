with player_upcoming_games_combinations as (
    select
        cplt.player_id,
        pm.position,
        nug.fpl_game_id,  
        nug.datetime,     
        nug.team_id,
        nug.opponent_team_id,
        nug.is_home
    from {{ ref('current_players_last_team') }} cplt
    join {{ ref('next_upcoming_games') }} nug
        on cplt.team_id = nug.team_id
    join {{ ref('player_mappings') }} pm
        on cplt.player_id = pm.player_id
        and nug.season = pm.season
)
, player_data_rn as (
    select
        *,
        row_number() over(
            partition by player_id
            order by datetime desc
        ) as rn
    from {{ ref('da_player_games') }}
)
, player_data_rolling_last as (
    select *
    from player_data_rn
    where rn = 1
)
, game_data_rn as (
    select
        *,
        row_number() over(
            partition by team_id, opponent_team_id
            order by datetime desc
        ) as rn
    from {{ ref('da_games') }}
)
, game_data_rolling_last as (
    select *
    from game_data_rn
    where rn = 1
)
, prediction_input as (
    select 
        pugc.player_id,
        pugc.position,
        pugc.fpl_game_id,
        pugc.datetime,
        pugc.team_id,
        pugc.opponent_team_id,
        pugc.is_home,
        {% for metric in var('player_games_rolling_metrics') %}
            coalesce(pdrl.{{ metric }}_last_5, 0) as {{ metric }}_last_5,
            coalesce(pdrl.{{ metric }}_overall, 0) as {{ metric }}_overall,
            coalesce(pdrl.{{ metric }}_vs_opponent, 0) as {{ metric }}_vs_opponent,
        {% endfor %}
        coalesce(gdrl.win_rate_prior, 0) as win_rate_prior,
        coalesce(gdrl.win_rate_last_5_prior, 0) as win_rate_last_5_prior,
        coalesce(gdrl.win_rate_vs_opponent_prior, 0) as win_rate_vs_opponent_prior,
        coalesce(gdrl.total_games_vs_opponent_prior, 0) as total_games_vs_opponent_prior,
        coalesce(gdrl.avg_goals_scored_prior, 0) as avg_goals_scored_prior,
        coalesce(gdrl.avg_expected_goals_prior, 0) as avg_expected_goals_prior,
        coalesce(gdrl.avg_goals_scored_last_5_prior, 0) as avg_goals_scored_last_5_prior,
        coalesce(gdrl.avg_expected_goals_last_5_prior, 0) as avg_expected_goals_last_5_prior,
        coalesce(gdrl.avg_goals_scored_vs_opponent_prior, 0) as avg_goals_scored_vs_opponent_prior,
        coalesce(gdrl.avg_expected_goals_vs_opponent_prior, 0) as avg_expected_goals_vs_opponent_prior
    from player_upcoming_games_combinations pugc
    left join player_data_rolling_last pdrl
        on pugc.player_id = pdrl.player_id
    left join game_data_rolling_last gdrl
        on gdrl.team_id = pugc.team_id
        and gdrl.opponent_team_id = pugc.opponent_team_id
)

select *
from prediction_input