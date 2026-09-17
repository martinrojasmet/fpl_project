with active_teams as (
    select id as team_id
    from {{ ref('teams') }}
    where is_active = true
)
, player_games_teams as (
    select
        pg.id as player_game_id,
        pg.game_id,
        cplt.team_id,
        case 
            when cplt.team_id = g.home_team_id then g.away_team_id
            else g.home_team_id
        end as opponent_team_id,
        pg.goals_scored,
        pg.expected_goals_understat,
        pg.goals_conceded
    from {{ ref('player_games') }} pg
    join {{ ref('current_players_last_team') }} cplt
        on pg.player_id = cplt.player_id
    join {{ ref('games') }} g
        on pg.game_id = g.id
)
, team_game_goals as (
    select
        game_id,
        team_id,
        sum(goals_scored) as total_goals_scored,
        sum(expected_goals_understat) as total_expected_goals,
        sum(goals_conceded) as total_goals_conceded
    from player_games_teams
    group by 1, 2
)
, unpivoted_games as (
    select
        g.id as game_id,
        g.season,
        g.gameweek,
        g.datetime,
        g.home_team_id as team_id,
        g.away_team_id as opponent_team_id,
        true as is_home,
        case
            when g.result = 'home_win' then 1
            else 0
        end as is_win,
        case
            when g.result = 'tie' then 1
            else 0
        end as is_tie,
        tgg.total_goals_scored,
        tgg.total_expected_goals,
        tgg.total_goals_conceded
    from {{ ref('games') }} g
    left join team_game_goals tgg 
        on g.id = tgg.game_id and g.home_team_id = tgg.team_id

    union all

    select
        g.id as game_id,
        g.season,
        g.gameweek,
        g.datetime,
        g.away_team_id as team_id,
        g.home_team_id as opponent_team_id,
        false as is_home,
        case
            when g.result = 'away_win' then 1
            else 0
        end as is_win,
        case
            when g.result = 'tie' then 1
            else 0
        end as is_tie,
        tgg.total_goals_scored,
        tgg.total_expected_goals,
        tgg.total_goals_conceded
    from {{ ref('games') }} g
    left join team_game_goals tgg 
        on g.id = tgg.game_id and g.away_team_id = tgg.team_id
)
, da_games as (
    select
        game_id,
        datetime,
        team_id,
        opponent_team_id,
        is_win,
        is_tie,
        avg(is_win) over (
            partition by team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as win_rate_prior,
        avg(is_win) over (
            partition by team_id 
            order by datetime, game_id 
            rows between 5 preceding and 1 preceding
        ) as win_rate_last_5_prior,
        avg(is_win) over (
            partition by team_id, opponent_team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as win_rate_vs_opponent_prior,
        count(is_win) over (
            partition by team_id, opponent_team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as total_games_vs_opponent_prior,
        avg(total_goals_scored) over (
            partition by team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as avg_goals_scored_prior,
        avg(total_expected_goals) over (
            partition by team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as avg_expected_goals_prior,
        avg(total_goals_scored) over (
            partition by team_id 
            order by datetime, game_id 
            rows between 5 preceding and 1 preceding
        ) as avg_goals_scored_last_5_prior,
        avg(total_expected_goals) over (
            partition by team_id 
            order by datetime, game_id 
            rows between 5 preceding and 1 preceding
        ) as avg_expected_goals_last_5_prior,
        avg(total_goals_scored) over (
            partition by team_id, opponent_team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as avg_goals_scored_vs_opponent_prior,
        avg(total_expected_goals) over (
            partition by team_id, opponent_team_id 
            order by datetime, game_id 
            rows between unbounded preceding and 1 preceding
        ) as avg_expected_goals_vs_opponent_prior
    from unpivoted_games
)

select * from da_games
