{% test expect_length_gt(model, column_name, length) %}
    SELECT 1 FROM {{ model }}
    WHERE ST_Length({{ column_name }}) <= {{ length }}
{% endtest %}

{% test expect_length_gte(model, column_name, length) %}
    SELECT 1 FROM {{ model }}
    WHERE ST_Length({{ column_name }}) < {{ length }}
{% endtest %}

{% test expect_lay_within_polygon(model, column_name, polygon_wkt) %}
    SELECT 1 FROM {{ model }}
    WHERE NOT ST_Within({{ column_name}}, ST_GeomFromText('{{ polygon_wkt }}' ))
{% endtest %}

{% test expect_intersects_with_polygon(model, column_name, polygon_wkt) %}
    SELECT 1 FROM {{ model }}
    WHERE NOT ST_Intersects({{ column_name}}, ST_GeomFromText('{{ polygon_wkt }}' ))
{% endtest %}

{% test has_epsg(model, column_name, epsg_code) %}
    SELECT 1 FROM {{ model }}
    WHERE ST_SRID({{ column_name }}) <> {{ epsg_code }}
{% endtest %}

{% test is_simple(model, column_name) %}
    SELECT 1 FROM {{ model }}
    WHERE NOT ST_IsSimple({{ column_name }})
{% endtest %}

{% test is_not_simple(model, column_name) %}
    SELECT 1 FROM {{ model }}
    WHERE ST_IsSimple({{ column_name }})
{% endtest %}

{% test is_valid(model, column_name) %}
    SELECT 1 FROM {{ model }}
    WHERE NOT ST_IsValid({{ column_name }})
{% endtest %}

{% test is_closed(model, column_name) %}
    SELECT 1 FROM {{ model }}
    WHERE NOT ST_IsClosed({{ column_name }})
{% endtest %}

{% test is_empty(model, column_name) %}
    SELECT 1 FROM {{ model }}
    WHERE NOT ST_IsClosed({{ column_name }})
{% endtest %}

{% test has_valid_geographic_coordinates(model, column_name) %}
    WITH geometry_type AS (
        SELECT ST_GeometryType({{ column_name }}) AS geo_type, {{ column_name }}
        FROM {{ model }}
    ),
    IS_VALID AS (
        SELECT ST_Contains(ST_GeomFromText('POLYGON(( -180 -90, -180 90, 180 90, 180 -90, -180 -90))'), {{ column_name}}) AS is_within
        FROM geometry_type
    )
    SELECT 1 FROM IS_VALID
    WHERE NOT is_within
{% endtest %}

{% test expect_area_gt %}
    SELECT 1 FROM {{ model }}
    WHERE ST_Area({{ column_name }}) <= {{ length }}
{% endtest %}

{% test expect_area_gte %}
    SELECT 1 FROM {{ model }}
    WHERE ST_Length({{ column_name }}) < {{ length }}
{% endtest %}
