with prediction_input as (
    select
        pg.player_id,
        pg.position,
        pg.game_id,
        pg.datetime,
        pg.team_id,
        pg.opponent_team_id,
        pg.is_home,
        pg.points,
        {% for metric in var('player_games_rolling_metrics') %}
            coalesce(pg.{{ metric }}_last_5, 0) as {{ metric }}_last_5,
            coalesce(pg.{{ metric }}_overall, 0) as {{ metric }}_overall,
            coalesce(pg.{{ metric }}_vs_opponent, 0) as {{ metric }}_vs_opponent,
        {% endfor %}
        coalesce(g.win_rate_prior, 0) as win_rate_prior,
        coalesce(g.win_rate_last_5_prior, 0) as win_rate_last_5_prior,
        coalesce(g.win_rate_vs_opponent_prior, 0) as win_rate_vs_opponent_prior,
        coalesce(g.total_games_vs_opponent_prior, 0) as total_games_vs_opponent_prior,
        coalesce(g.avg_goals_scored_prior, 0) as avg_goals_scored_prior,
        coalesce(g.avg_expected_goals_prior, 0) as avg_expected_goals_prior,
        coalesce(g.avg_goals_scored_last_5_prior, 0) as avg_goals_scored_last_5_prior,
        coalesce(g.avg_expected_goals_last_5_prior, 0) as avg_expected_goals_last_5_prior,
        coalesce(g.avg_goals_scored_vs_opponent_prior, 0) as avg_goals_scored_vs_opponent_prior,
        coalesce(g.avg_expected_goals_vs_opponent_prior, 0) as avg_expected_goals_vs_opponent_prior
    from {{ ref('da_player_games')}} pg
    join {{ ref('da_games') }} g
        on g.game_id = pg.game_id
        and g.team_id = pg.team_id
)

select *
from prediction_input