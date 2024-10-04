-- File: macros/text_utils.sql

{% macro clean_text(column, case='lower') %}
    {% if case == 'lower' %}
        regexp_replace(trim(lower({{ column }})), '[[:punct:]]', '')
    {% elif case == 'upper' %}
        regexp_replace(trim(upper({{ column }})), '[[:punct:]]', '')
    {% else %}
        regexp_replace(trim({{ column }}), '[[:punct:]]', '')
    {% endif %}
{% endmacro %}
