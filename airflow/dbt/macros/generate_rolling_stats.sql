{% macro generate_rolling_stats(metrics) %}
    {% for metric in metrics %}
        -- Last 5 Games Form
        avg({{ metric }}) over (
            partition by player_id 
            order by datetime 
            rows between 5 preceding and 1 preceding
        ) as {{ metric }}_last_5,

        -- Career / Overall Baseline
        avg({{ metric }}) over (
            partition by player_id 
            order by datetime 
            rows between unbounded preceding and 1 preceding
        ) as {{ metric }}_overall,

        -- Head-to-Head vs Opponent
        avg({{ metric }}) over (
            partition by player_id, opponent_team_id 
            order by datetime 
            rows between unbounded preceding and 1 preceding
        ) as {{ metric }}_vs_opponent{% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}