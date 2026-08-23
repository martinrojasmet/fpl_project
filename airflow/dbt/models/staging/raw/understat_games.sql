{{
config(
    materialized="view"
)
}}

select
    id, 
    understat_id,
    date,
    home,
    away,
    {{ season_from_date('date') }} as season
from {{ source('raw', 'understat_games') }}