{{ config(
    unique_key='id',
    incremental_strategy='merge'
) }}

with initial_teams as (
    {% if not is_incremental() %}
    select
        id,
        name
    from {{ ref('fpl_db_core_teams') }}
    {% else %}
    select
        id,
        name
    from {{ this }}
    {% endif %}
)
, ranked_teams as (
    select
        team_id as id,
        trim(fpl_name) as name,
        row_number() over (
            partition by team_id 
            order by 
                case when season = {{ fpl_current_season() }} then 1 else 2 end,
                season desc
        ) as rn
    from {{ ref('team_mappings') }}
)
, pipeline_teams as (
    select
        id,
        name
    from ranked_teams
    where rn = 1
)
, all_teams as (
    select
        coalesce(i.id, p.id) as id,
        coalesce(i.name, p.name) as name
    from pipeline_teams p
    {% if not is_incremental() %}
    full outer join initial_teams i
    {% else %}
    left join initial_teams i
    {% endif %}
        on i.id = p.id
)

select
    d.id,
    d.name
from all_teams d