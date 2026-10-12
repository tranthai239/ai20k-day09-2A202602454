{# DuckDB is only the engine. The data lives in the lake as Delta tables; these views point at them.
   A view is created only if its Delta table already exists, so dbt also runs early in the lab (before silver.frames is built). #}
{% macro create_lake_views() %}
  {% if execute %}
    {% set bucket = env_var('LAKE_BUCKET', 'lake') %}
    {% set endpoint = env_var('S3_ENDPOINT', 'http://localhost:9000').split('://')[1] %}
    {% do run_query("create or replace secret lake (type s3, key_id '" ~ env_var('S3_ACCESS_KEY', 'minioadmin') ~ "', secret '" ~ env_var('S3_SECRET_KEY', 'minioadmin') ~ "', endpoint '" ~ endpoint ~ "', url_style 'path', use_ssl false, region 'us-east-1')") %}
    {% do run_query("create schema if not exists lake") %}
    {% for t in [('bronze','raw_telemetry_a'), ('bronze','raw_telemetry_b'), ('bronze','trips_raw'), ('bronze','video_files'), ('silver','frames')] %}
      {% set base = "s3://" ~ bucket ~ "/" ~ t[0] ~ "/delta/" ~ t[1] %}
      {% set n = run_query("select count(*) from glob('" ~ base ~ "/_delta_log/*.json')").columns[0].values()[0] %}
      {% if n > 0 %}
        {% do run_query("create or replace view lake." ~ t[1] ~ " as select * from delta_scan('" ~ base ~ "')") %}
      {% endif %}
    {% endfor %}
  {% endif %}
  select 1
{% endmacro %}
