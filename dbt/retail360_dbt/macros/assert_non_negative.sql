{% test no_negative_amounts(model, column_name) %}
    -- This test will FAIL if the query below returns ANY rows. 
    -- We consider it a failure if {{ column_name }} < 0.
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endtest %}
