{# Small reusable data-quality tests. A test returns the BAD rows: zero rows = pass. #}
{% test in_range(model, column_name, min_value, max_value) %}
  select * from {{ model }} where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}

{% test unique_combination(model, columns) %}
  select {{ columns | join(', ') }}, count(*) as n from {{ model }} group by {{ columns | join(', ') }} having count(*) > 1
{% endtest %}

{# every `group_column` value may appear under only ONE `value_column` value: used to prove no trip sits in two splits #}
{% test no_straddle(model, column_name, across) %}
  select {{ column_name }}, count(distinct {{ across }}) as n from {{ model }} group by {{ column_name }} having count(distinct {{ across }}) > 1
{% endtest %}

{# largest allowed gap (ms) between consecutive rows per partition; use severity: warn for "completeness" signals #}
{% test max_gap_ms(model, column_name, partition_by, max_ms) %}
  select * from (
    select {{ partition_by }}, {{ column_name }},
           date_diff('millisecond', lag({{ column_name }}) over (partition by {{ partition_by }} order by {{ column_name }}), {{ column_name }}) as gap_ms
    from {{ model }}) where gap_ms > {{ max_ms }}
{% endtest %}

{# share of rows where a boolean column is true must stay below a limit #}
{% test max_share(model, column_name, max_share) %}
  select share from (select avg(case when {{ column_name }} then 1.0 else 0.0 end) as share from {{ model }}) where share > {{ max_share }}
{% endtest %}
