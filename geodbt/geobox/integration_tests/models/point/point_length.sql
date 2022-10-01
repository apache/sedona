{{
    config(materialized="table")
}}
WITH POINTS AS (
    SELECT ST_GeomFromText('POINT(21 52)') AS geom
)
SELECT * FROM POINTS