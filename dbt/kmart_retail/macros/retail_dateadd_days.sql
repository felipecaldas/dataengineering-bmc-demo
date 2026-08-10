{% macro retail_dateadd_days(date_expression, days) -%}
  {{ return(adapter.dispatch('retail_dateadd_days', 'kmart_retail')(date_expression, days)) }}
{%- endmacro %}

{% macro default__retail_dateadd_days(date_expression, days) -%}
  ({{ date_expression }} + ({{ days }}) * interval '1 day')
{%- endmacro %}

{% macro databricks__retail_dateadd_days(date_expression, days) -%}
  date_add({{ date_expression }}, {{ days }})
{%- endmacro %}

