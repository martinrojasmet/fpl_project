{% macro season_from_date(date_expr='current_date') %}
  case
    when extract(month from {{ date_expr }}) >= 8
      then concat(extract(year from {{ date_expr }})::text, '/', right((extract(year from {{ date_expr }}) + 1)::text, 2))
    else concat((extract(year from {{ date_expr }}) - 1)::text, '/', right((extract(year from {{ date_expr }}) )::text, 2))
  end
{% endmacro %}

{% macro fpl_current_season() %}
  {{ season_from_date() }}
{% endmacro %}