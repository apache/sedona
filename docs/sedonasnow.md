# SedonaSnow

SedonaSnow brings 200+ Apache Sedona geospatial functions directly into your Snowflake environment to complement the native Snowflake spatial functions.

## Key Advantages

* **200+ spatial functions**: Such as 3D distance, geometry validation, precision reduction
* **Fast spatial joins**: Sedona has special optimizations for performant spatial joins
* **Seamless integration**: Works alongside Snowflake's native functions
* **No data movement**: Everything stays in Snowflake

## Get Started

Here’s an example of how to run some queries on Snowflake tables with SedonaSnow.

```sql
USE DATABASE SEDONASNOW;

SELECT SEDONA.ST_GeomFromWKT(wkt) AS geom 
FROM your_table;

SELECT SEDONA.ST_3DDistance(geom1, geom2) FROM spatial_data;
```

Here’s an example of a spatial join:

```sql
SELECT * FROM lefts, rights
WHERE lefts.cellId = rights.cellId;
```

You can see how SedonaSnow seamlessly integrates into your current Snowflake environment.

## Next steps

SedonaSnow is an excellent option if you're doing serious spatial analysis in Snowflake.  It is fast and provides a wide range of spatial functions.  SedonaSnow removes the limitations of Snowflake's built-in spatial functions without forcing you to move your data to another platform.
