{{ config(
    unique_key='game_id',
    incremental_strategy='merge'
) }}

with intial_gm as (
    {% if not is_incremental() %}
    select
        id,
        game_id,
        fpl_game_id,
        understat_game_id
    from {{ ref('fpl_db_master_game_mappings') }}
    {% else %}
    select
        id,
        game_id,
        fpl_game_id,
        understat_game_id
    from {{ this }}
    {% endif %}
)
, max_id as (
    select max(id) as max_id
    from intial_gm
)
, dist_fpl_games as (
    select distinct
        fg.season,
        fg.fpl_game_id,
        cg.id as game_id
    from {{ ref('fpl_games') }} fg
    inner join {{ ref('fpl_team_mappings') }} tm_home 
        on fg.home_fpl_team_id = tm_home.fpl_team_id and fg.season = tm_home.season
    inner join {{ ref('fpl_team_mappings') }} tm_away 
        on fg.away_fpl_team_id = tm_away.fpl_team_id and fg.season = tm_away.season
    inner join {{ ref('games') }} cg 
        on fg.season = cg.season 
        and tm_home.team_id = cg.home_team_id 
        and tm_away.team_id = cg.away_team_id
)
, dist_understat_games as (
    select distinct
        ug.season,
        ug.understat_id as understat_game_id,
        cg.id as game_id
    from {{ ref('understat_games') }} ug
    inner join {{ ref('understat_team_mappings') }} tm_home 
        on ug.home = tm_home.name 
        and ug.season = tm_home.season
    inner join {{ ref('understat_team_mappings') }} tm_away 
        on ug.away = tm_away.name 
        and ug.season = tm_away.season
    inner join {{ ref('games') }} cg 
        on ug.season = cg.season 
        and tm_home.team_id = cg.home_team_id 
        and tm_away.team_id = cg.away_team_id
)
, pipeline_gm as (
    select
        coalesce(fg.season, ug.season) as season,
        coalesce(fg.game_id, ug.game_id) as game_id,
        fg.fpl_game_id,
        ug.understat_game_id
    from dist_fpl_games fg
    full outer join dist_understat_games ug
        on fg.season = ug.season 
       and fg.game_id = ug.game_id
)
, all_gm as (
    select
        d.id,
        coalesce(d.game_id, p.game_id) as game_id,
        coalesce(d.fpl_game_id, p.fpl_game_id) as fpl_game_id,
        coalesce(d.understat_game_id, p.understat_game_id) as understat_game_id,
        row_number() over() as rn
    from pipeline_gm p
    {% if not is_incremental() %}
    full outer join intial_gm d
    {% else %}
    left join intial_gm d
    {% endif %}
        on d.game_id = p.game_id
)
, all_gm_rn as (
    select
        *,
        case 
            when id is null then
                row_number() over ( 
                    partition by case when id is null then 1 else 0 end
                    order by rn
                )
            else null
        end as new_rn
    from all_gm
)
, final_gm as (
    select
        coalesce(a.id, m.max_id + a.new_rn) as id,
        a.game_id,
        a.fpl_game_id,
        a.understat_game_id
    from all_gm_rn a
    cross join max_id m
)

select *
from final_gm
