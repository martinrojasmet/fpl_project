{{ config(
    unique_key=['player_id', 'game_id'],
    incremental_strategy='merge'
) }}

with initial_pg as (
    {% if not is_incremental() %}
    select
        id,
        player_id,
        game_id,
        fpl_price,
        points,
        minutes_played,
        goals_scored,
        expected_goals_understat,
        goals_conceded,
        assists,
        expected_assists_understat,
        yellow_cards,
        red_card,
        clean_sheet,
        key_passes,
        own_goals,
        penalties_missed,
        penalties_saved,
        saves,
        bonus_points,
        expected_assists_fpl,
        expected_goals_fpl,
        fpl_element,
        local_understat_id
    from {{ ref('fpl_db_core_player_games') }}
    {% else %}
    select
        id,
        player_id,
        game_id,
        fpl_price,
        points,
        minutes_played,
        goals_scored,
        expected_goals_understat,
        goals_conceded,
        assists,
        expected_assists_understat,
        yellow_cards,
        red_card,
        clean_sheet,
        key_passes,
        own_goals,
        penalties_missed,
        penalties_saved,
        saves,
        bonus_points,
        expected_assists_fpl,
        expected_goals_fpl,
        fpl_element,
        local_understat_id
    from {{ this }}
    {% endif %}
)
, fpl_pg as (
    select
        pm.player_id,
        gm.game_id,
        fpg.fpl_element,
        fpg.value as fpl_price,
        fpg.total_points as points,
        fpg.minutes_played,
        fpg.goals_scored,
        fpg.goals_conceded,
        fpg.assists,
        fpg.yellow_cards,
        fpg.red_cards as red_card,
        fpg.clean_sheets as clean_sheet,
        fpg.own_goals,
        fpg.penalties_missed,
        fpg.penalties_saved,
        fpg.saves,
        fpg.bonus_points,
        fpg.expected_assists as expected_assists_fpl,
        fpg.expected_goals as expected_goals_fpl
    from {{ ref('fpl_player_games') }} fpg
    inner join {{ ref('player_mappings') }} pm
        on fpg.opta_id = pm.opta_id
       and fpg.season = pm.season
    inner join {{ ref('game_mappings') }} gm
        on fpg.fpl_game_id = gm.fpl_game_id
)
, understat_pg as (
    select
        pm.player_id,
        gm.game_id as game_id,
        upg.understat_game_id as local_understat_id,
        upg.expected_goals as expected_goals_understat,
        upg.expected_assists as expected_assists_understat,
        upg.key_passes
    from {{ ref('understat_player_games') }} upg
    inner join {{ ref('understat_games') }} ug
        on upg.understat_game_id = ug.understat_id
    inner join {{ ref('player_mappings') }} pm
        on upg.name = pm.understat_name
        and ug.season = pm.season
    inner join {{ ref('game_mappings') }} gm
        on upg.understat_game_id = gm.understat_game_id
)
, pipeline_pg as (
    select
        gen_random_uuid() as id,
        f.player_id,
        f.game_id,
        f.fpl_element,
        u.local_understat_id,
        f.fpl_price,
        f.points,
        f.minutes_played,
        f.goals_scored,
        coalesce(u.expected_goals_understat, 0.0) as expected_goals_understat,
        f.goals_conceded,
        f.assists,
        coalesce(u.expected_assists_understat, 0.0) as expected_assists_understat,
        f.yellow_cards,
        f.red_card,
        f.clean_sheet,
        u.key_passes,
        f.own_goals,
        f.penalties_missed,
        f.penalties_saved,
        f.saves,
        f.bonus_points,
        f.expected_assists_fpl,
        f.expected_goals_fpl
    from fpl_pg f
    left join understat_pg u
        on f.player_id = u.player_id
       and f.game_id = u.game_id
    where u.player_id is not null
       or f.minutes_played = 0
)
, all_pg as (
    select
        coalesce(i.id, p.id) as id,
        coalesce(p.player_id, i.player_id) as player_id,
        coalesce(p.game_id, i.game_id) as game_id,
        coalesce(p.fpl_price, i.fpl_price) as fpl_price,
        coalesce(p.points, i.points) as points,
        coalesce(p.minutes_played, i.minutes_played) as minutes_played,
        coalesce(p.goals_scored, i.goals_scored) as goals_scored,
        coalesce(p.expected_goals_understat, i.expected_goals_understat) as expected_goals_understat,
        coalesce(p.goals_conceded, i.goals_conceded) as goals_conceded,
        coalesce(p.assists, i.assists) as assists,
        coalesce(p.expected_assists_understat, i.expected_assists_understat) as expected_assists_understat,
        coalesce(p.yellow_cards, i.yellow_cards) as yellow_cards,
        coalesce(p.red_card, i.red_card) as red_card,
        coalesce(p.clean_sheet, i.clean_sheet) as clean_sheet,
        coalesce(p.key_passes, i.key_passes) as key_passes,
        coalesce(p.own_goals, i.own_goals) as own_goals,
        coalesce(p.penalties_missed, i.penalties_missed) as penalties_missed,
        coalesce(p.penalties_saved, i.penalties_saved) as penalties_saved,
        coalesce(p.saves, i.saves) as saves,
        coalesce(p.bonus_points, i.bonus_points) as bonus_points,
        coalesce(p.expected_assists_fpl, i.expected_assists_fpl) as expected_assists_fpl,
        coalesce(p.expected_goals_fpl, i.expected_goals_fpl) as expected_goals_fpl,
        coalesce(p.fpl_element, i.fpl_element) as fpl_element,
        coalesce(p.local_understat_id, i.local_understat_id) as local_understat_id
    from pipeline_pg p
    {% if not is_incremental() %}
    full outer join initial_pg i
    {% else %}
    left join initial_pg i
    {% endif %}
        on i.player_id = p.player_id
       and i.game_id = p.game_id
)

select *
from all_pg