{% macro clarion_time(column) %}
    case
        when {{ column }} is null or {{ column }} = 0 then null
        else time(
            div({{ column }}, 360000),
            mod(div({{ column }}, 6000), 60),
            mod(div({{ column }}, 100), 60)
        )
    end
{% endmacro %}
