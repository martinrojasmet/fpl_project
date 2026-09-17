with prediction_input as (
    select
        pg.player_id,
        pg.position,
        pg.game_id,
        pg.datetime,
        EXTRACT(YEAR FROM pg.datetime) AS year,
        EXTRACT(MONTH FROM pg.datetime) AS month,
        EXTRACT(DAY FROM pg.datetime) AS day,
        EXTRACT(HOUR FROM pg.datetime) AS hour,
        pg.team_id,
        pg.opponent_team_id,
        pg.is_home,
        pg.points,
        {% for metric in var('player_games_rolling_metrics') %}
            pg.{{ metric }}_last_5,
            pg.{{ metric }}_overall,
            pg.{{ metric }}_vs_opponent,
        {% endfor %}
        g.win_rate_prior,
        g.win_rate_last_5_prior,
        g.win_rate_vs_opponent_prior,
        g.total_games_vs_opponent_prior,
        g.avg_goals_scored_prior,
        g.avg_expected_goals_prior,
        g.avg_goals_scored_last_5_prior,
        g.avg_expected_goals_last_5_prior,
        g.avg_goals_scored_vs_opponent_prior,
        g.avg_expected_goals_vs_opponent_prior
    from {{ ref('da_player_games')}} pg
    join {{ ref('da_games') }} g
        on g.game_id = pg.game_id
        and g.team_id = pg.team_id
)

select *
from prediction_input