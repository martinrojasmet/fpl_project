with backwards_position_propagation as (
    select
        ps.id,
        ps.season,
        ps.player_id,
        coalesce(
            ps.position,
            first_value(ps.position) over (
                partition by ps.player_id
                order by
                    case when ps.position is not null then 0 else 1 end,
                    ps.season asc
            )
        ) as position
    from {{ ref('player_seasons') }} ps
)
, da_player_games as (
    select ps.player_id,
        ps.position,
        pg.game_id,
        g.datetime,
        pt.team_id,
        case
            when g.home_team_id = pt.team_id then g.away_team_id
            else g.home_team_id
        end as opponent_team_id,
        case
            when g.home_team_id = pt.team_id then true
            else false
        end as is_home,
        g.result,
        pg.fpl_price,
        pg.points,
        pg.minutes_played,
        pg.goals_scored,
        pg.expected_goals_understat,
        pg.goals_conceded,
        pg.assists,
        pg.expected_assists_understat,
        pg.yellow_cards,
        pg.red_card,
        pg.clean_sheet,
        case
            when (pg.key_passes is null) and (pg.minutes_played = 0)
                    then 0
                else pg.key_passes
            end as key_passes,
        pg.own_goals,
        pg.penalties_missed,
        pg.penalties_saved,
        pg.saves,
        pg.bonus_points
    from {{ ref('player_games') }} pg
    join {{ ref('games') }} g
        on pg.game_id = g.id
    join {{ ref('player_teams') }} pt
        on pg.player_id = pt.player_id
       and g.datetime >= pt.start_date 
       and (g.datetime < pt.end_date or pt.end_date is null)
    join backwards_position_propagation ps
        on pg.player_id = ps.player_id
        and g.season = ps.season
)
, da_player_games_with_rolling as (
    select *,
        {{ generate_rolling_stats(var('player_games_rolling_metrics')) }}
    from da_player_games
)

select *
from da_player_games_with_rolling
