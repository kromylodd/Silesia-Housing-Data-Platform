{#
    Default dbt behavior concatenates a custom +schema config onto the
    target's schema (e.g. staging_housing_marts_housing). This project uses
    three fixed, separately-provisioned BigQuery datasets (Terraform-managed:
    raw_housing / staging_housing / marts_housing), so a model's +schema
    should be used exactly as given instead.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
