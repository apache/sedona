!!!note
    Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

## ST_Envelope_Aggr

Introduction: Return the entire envelope boundary of all geometries in A

Format: `ST_Envelope_Aggr (A:geometryColumn)`

SQL example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') AS geom
)
SELECT sedona.ST_AsText(envelope)
FROM src_tbl,
     TABLE(sedona.ST_Envelope_Aggr(src_tbl.geom) OVER (PARTITION BY 1));
```

Output:

```
POLYGON ((0 0, 0 1.5, 1.5 1.5, 1.5 0, 0 0))
```

## ST_Intersection_Aggr

Introduction: Return the polygon intersection of all polygons in A

Format: `ST_Intersection_Aggr (A:geometryColumn)`

SQL example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') AS geom
)
SELECT sedona.ST_AsText(intersected)
FROM src_tbl,
     TABLE(sedona.ST_Intersection_Aggr(src_tbl.geom) OVER (PARTITION BY 1));
```

Output:

```
POLYGON ((0.5 1, 1 1, 1 0.5, 0.5 0.5, 0.5 1))
```

## ST_Union_Aggr

Introduction: Return the polygon union of all polygons in A

Format: `ST_Union_Aggr (A:geometryColumn)`

SQL example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') AS geom
)
SELECT sedona.ST_AsText(unioned)
FROM src_tbl,
     TABLE(sedona.ST_Union_Aggr(src_tbl.geom) OVER (PARTITION BY 1));
```

Output:

```
POLYGON ((0 0, 0 1, 0.5 1, 0.5 1.5, 1.5 1.5, 1.5 0.5, 1 0.5, 1 0, 0 0))
```
