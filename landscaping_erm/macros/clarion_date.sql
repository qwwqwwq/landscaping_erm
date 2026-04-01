{% macro clarion_date(column) %}
    case
        when {{ column }} is null or {{ column }} = 0 then null
        else date_add(date '1800-12-28', interval ({{ column }} - 1) day)
    end
{% endmacro %}
