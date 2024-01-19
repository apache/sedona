-- IsNativeApp is false. Generating DDL for User-Managed Snowflake Account
-- UDFs --
create or replace function sedona.ST_Affine (geometry BINARY, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE, f DOUBLE, g DOUBLE, h DOUBLE, i DOUBLE, xOff DOUBLE, yOff DOUBLE, zOff DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Affine'
;
create or replace function sedona.ST_Affine (geometry BINARY, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE, f DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Affine'
;
create or replace function sedona.ST_Angle (geom1 BINARY, geom2 BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Angle'
;
create or replace function sedona.ST_Angle (geom1 BINARY, geom2 BINARY, geom3 BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Angle'
;
create or replace function sedona.ST_Angle (geom1 BINARY, geom2 BINARY, geom3 BINARY, geom4 BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Angle'
;
create or replace function sedona.ST_Area (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Area'
;
create or replace function sedona.ST_AsEWKB (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsEWKB'
;
create or replace function sedona.ST_AsEWKT (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsEWKT'
;
create or replace function sedona.ST_AsGML (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsGML'
;
create or replace function sedona.ST_AsKML (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsKML'
;
create or replace function sedona.ST_Azimuth (left BINARY, right BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Azimuth'
;
create or replace function sedona.ST_Buffer (geometry BINARY, radius DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Buffer'
;
create or replace function sedona.ST_Covers (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Covers'
;
create or replace function sedona.ST_Crosses (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Crosses'
;
create or replace function sedona.ST_Degrees (angleInRadian DOUBLE)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Degrees'
;
create or replace function sedona.ST_Equals (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Equals'
;
create or replace function sedona.ST_Force2D (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Force2D'
;
create or replace function sedona.ST_GeoHash (geometry BINARY, precision NUMBER)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeoHash'
;
create or replace function sedona.ST_IsEmpty (geometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsEmpty'
;
create or replace function sedona.ST_IsRing (geometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsRing'
;
create or replace function sedona.ST_IsValid (geometry BINARY, flags NUMBER)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsValid'
;
create or replace function sedona.ST_IsValid (geometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsValid'
;
create or replace function sedona.ST_Length (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Length'
;
create or replace function sedona.ST_Multi (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Multi'
;
create or replace function sedona.ST_NDims (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_NDims'
;
create or replace function sedona.ST_NPoints (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_NPoints'
;
create or replace function sedona.ST_Point (x DOUBLE, y DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Point'
;
create or replace function sedona.ST_PointN (geometry BINARY, n NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PointN'
;
create or replace function sedona.ST_PointZ (x DOUBLE, y DOUBLE, z DOUBLE, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PointZ'
;
create or replace function sedona.ST_PointZ (x DOUBLE, y DOUBLE, z DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PointZ'
;
create or replace function sedona.ST_Polygon (geometry BINARY, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Polygon'
;
create or replace function sedona.ST_Reverse (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Reverse'
;
create or replace function sedona.ST_SRID (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_SRID'
;
create or replace function sedona.ST_AsText (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsText'
;
create or replace function sedona.ST_SetSRID (geometry BINARY, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_SetSRID'
;
create or replace function sedona.ST_Split (input BINARY, blade BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Split'
;
create or replace function sedona.ST_Touches (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Touches'
;
create or replace function sedona.ST_Union (leftGeom BINARY, rightGeom BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Union'
;
create or replace function sedona.ST_Within (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Within'
;
create or replace function sedona.ST_X (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_X'
;
create or replace function sedona.ST_XMax (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_XMax'
;
create or replace function sedona.ST_XMin (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_XMin'
;
create or replace function sedona.ST_Y (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Y'
;
create or replace function sedona.ST_YMax (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_YMax'
;
create or replace function sedona.ST_YMin (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_YMin'
;
create or replace function sedona.ST_Z (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Z'
;
create or replace function sedona.ST_ZMax (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ZMax'
;
create or replace function sedona.ST_ZMin (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ZMin'
;
create or replace function sedona.ST_DWithin (geomA BINARY, geomB BINARY, distance DOUBLE)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_DWithin'
;
create or replace function sedona.ST_Force3D (geom BINARY, zValue DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Force3D'
;
create or replace function sedona.ST_Force3D (geom BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Force3D'
;
create or replace function sedona.ST_NRings (geom BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_NRings'
;
create or replace function sedona.GeometryType (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.GeometryType'
;
create or replace function sedona.ST_AddPoint (linestring BINARY, point BINARY, position NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AddPoint'
;
create or replace function sedona.ST_AsBinary (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsBinary'
;
create or replace function sedona.ST_AsGeoJSON (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AsGeoJSON'
;
create or replace function sedona.ST_Boundary (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Boundary'
;
create or replace function sedona.ST_BoundingDiagonal (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_BoundingDiagonal'
;
create or replace function sedona.ST_BuildArea (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_BuildArea'
;
create or replace function sedona.ST_Centroid (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Centroid'
;
create or replace function sedona.ST_ClosestPoint (geometry1 BINARY, geometry2 BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ClosestPoint'
;
create or replace function sedona.ST_CollectionExtract (geometry BINARY, geomType NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_CollectionExtract'
;
create or replace function sedona.ST_CollectionExtract (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_CollectionExtract'
;
create or replace function sedona.ST_ConcaveHull (geometry BINARY, pctConvex DOUBLE, allowHoles BOOLEAN)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ConcaveHull'
;
create or replace function sedona.ST_ConcaveHull (geometry BINARY, pctConvex DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ConcaveHull'
;
create or replace function sedona.ST_Contains (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Contains'
;
create or replace function sedona.ST_CoordDim (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_CoordDim'
;
create or replace function sedona.ST_ConvexHull (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ConvexHull'
;
create or replace function sedona.ST_CoveredBy (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_CoveredBy'
;
create or replace function sedona.ST_Difference (leftGeometry BINARY, rightGeometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Difference'
;
create or replace function sedona.ST_Dimension (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Dimension'
;
create or replace function sedona.ST_Disjoint (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Disjoint'
;
create or replace function sedona.ST_Distance (left BINARY, right BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Distance'
;
create or replace function sedona.ST_3DDistance (left BINARY, right BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_3DDistance'
;
create or replace function sedona.ST_DumpPoints (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_DumpPoints'
;
create or replace function sedona.ST_EndPoint (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_EndPoint'
;
create or replace function sedona.ST_Envelope (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Envelope'
;
create or replace function sedona.ST_ExteriorRing (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ExteriorRing'
;
create or replace function sedona.ST_FlipCoordinates (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_FlipCoordinates'
;
create or replace function sedona.ST_Force_2D (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Force_2D'
;
create or replace function sedona.ST_GeomFromGML (gml VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromGML'
;
create or replace function sedona.ST_GeomFromGeoHash (geoHash VARCHAR, precision NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromGeoHash'
;
create or replace function sedona.ST_GeomFromGeoJSON (geoJson VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromGeoJSON'
;
create or replace function sedona.ST_GeomFromKML (kml VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromKML'
;
create or replace function sedona.ST_GeomFromText (wkt VARCHAR, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromText'
;
create or replace function sedona.ST_GeomFromText (wkt VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromText'
;
create or replace function sedona.ST_GeomFromWKB (wkb BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromWKB'
;
create or replace function sedona.ST_GeomFromWKT (wkt VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromWKT'
;
create or replace function sedona.ST_GeomFromWKT (wkt VARCHAR, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeomFromWKT'
;
create or replace function sedona.ST_GeometryN (geometry BINARY, n NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeometryN'
;
create or replace function sedona.ST_GeometryType (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeometryType'
;
create or replace function sedona.ST_HausdorffDistance (geom1 BINARY, geom2 BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_HausdorffDistance'
;
create or replace function sedona.ST_HausdorffDistance (geom1 BINARY, geom2 BINARY, densifyFrac DOUBLE)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_HausdorffDistance'
;
create or replace function sedona.ST_InteriorRingN (geometry BINARY, n NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_InteriorRingN'
;
create or replace function sedona.ST_Intersection (leftGeometry BINARY, rightGeometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Intersection'
;
create or replace function sedona.ST_Intersects (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Intersects'
;
create or replace function sedona.ST_IsClosed (geometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsClosed'
;
create or replace function sedona.ST_IsCollection (geometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsCollection'
;
create or replace function sedona.ST_IsSimple (geometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsSimple'
;
create or replace function sedona.ST_IsValidReason (geometry BINARY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsValidReason'
;
create or replace function sedona.ST_IsValidReason (geometry BINARY, flags NUMBER)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_IsValidReason'
;
create or replace function sedona.ST_LineFromMultiPoint (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineFromMultiPoint'
;
create or replace function sedona.ST_LineFromText (geomString VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineFromText'
;
create or replace function sedona.ST_LineInterpolatePoint (geom BINARY, fraction DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineInterpolatePoint'
;
create or replace function sedona.ST_LineLocatePoint (geom BINARY, point BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineLocatePoint'
;
create or replace function sedona.ST_LineMerge (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineMerge'
;
create or replace function sedona.ST_LineStringFromText (geomString VARCHAR, delimiter VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineStringFromText'
;
create or replace function sedona.ST_LineSubstring (geom BINARY, fromFraction DOUBLE, toFraction DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LineSubstring'
;
create or replace function sedona.ST_MakeLine (geometryCollection BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakeLine'
;
create or replace function sedona.ST_MakeLine (point1 BINARY, point2 BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakeLine'
;
create or replace function sedona.ST_MakePoint (x DOUBLE, y DOUBLE, z DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakePoint'
;
create or replace function sedona.ST_MakePoint (x DOUBLE, y DOUBLE, z DOUBLE, m DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakePoint'
;
create or replace function sedona.ST_MakePoint (x DOUBLE, y DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakePoint'
;
create or replace function sedona.ST_MLineFromText (wkt VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MLineFromText'
;
create or replace function sedona.ST_MLineFromText (wkt VARCHAR, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MLineFromText'
;
create or replace function sedona.ST_MPolyFromText (wkt VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MPolyFromText'
;
create or replace function sedona.ST_MPolyFromText (wkt VARCHAR, srid NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MPolyFromText'
;
create or replace function sedona.ST_MakePolygon (shell BINARY, holes BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakePolygon'
;
create or replace function sedona.ST_MakePolygon (shell BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakePolygon'
;
create or replace function sedona.ST_MakeValid (geometry BINARY, keepCollapsed BOOLEAN)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakeValid'
;
create or replace function sedona.ST_MakeValid (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MakeValid'
;
create or replace function sedona.ST_MinimumBoundingCircle (geometry BINARY, quadrantSegments NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_MinimumBoundingCircle'
;
create or replace function sedona.ST_Normalize (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Normalize'
;
create or replace function sedona.ST_NumGeometries (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_NumGeometries'
;
create or replace function sedona.ST_NumInteriorRings (geometry BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_NumInteriorRings'
;
create or replace function sedona.ST_OrderingEquals (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_OrderingEquals'
;
create or replace function sedona.ST_Overlaps (leftGeometry BINARY, rightGeometry BINARY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Overlaps'
;
create or replace function sedona.ST_PointFromText (geomString VARCHAR, geomFormat VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PointFromText'
;
create or replace function sedona.ST_PointOnSurface (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PointOnSurface'
;
create or replace function sedona.ST_PolygonFromEnvelope (minX DOUBLE, minY DOUBLE, maxX DOUBLE, maxY DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PolygonFromEnvelope'
;
create or replace function sedona.ST_PolygonFromText (geomString VARCHAR, delimiter VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PolygonFromText'
;
create or replace function sedona.ST_PrecisionReduce (geometry BINARY, precisionScale NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_PrecisionReduce'
;
create or replace function sedona.ST_ReducePrecision (geometry BINARY, precisionScale NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_ReducePrecision'
;
create or replace function sedona.ST_RemovePoint (linestring BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_RemovePoint'
;
create or replace function sedona.ST_RemovePoint (linestring BINARY, position NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_RemovePoint'
;
create or replace function sedona.ST_S2CellIDs (input BINARY, level NUMBER)
returns ARRAY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_S2CellIDs'
;
create or replace function sedona.ST_SetPoint (linestring BINARY, position NUMBER, point BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_SetPoint'
;
create or replace function sedona.ST_StartPoint (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_StartPoint'
;
create or replace function sedona.ST_SubDivide (geometry BINARY, maxVertices NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_SubDivide'
;
create or replace function sedona.ST_SymDifference (leftGeom BINARY, rightGeom BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_SymDifference'
;
create or replace function sedona.ST_Transform (geometry BINARY, sourceCRS VARCHAR, targetCRS VARCHAR, lenient BOOLEAN)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Transform'
;
create or replace function sedona.ST_Transform (geometry BINARY, sourceCRS VARCHAR, targetCRS VARCHAR)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Transform'
;
create or replace function sedona.ST_VoronoiPolygons (geometry BINARY, tolerance DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_VoronoiPolygons'
;
create or replace function sedona.ST_VoronoiPolygons (geometry BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_VoronoiPolygons'
;
create or replace function sedona.ST_VoronoiPolygons (geometry BINARY, tolerance DOUBLE, extent BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_VoronoiPolygons'
;
create or replace function sedona.ST_AreaSpheroid (geometry BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_AreaSpheroid'
;
create or replace function sedona.ST_DistanceSphere (geomA BINARY, geomB BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_DistanceSphere'
;
create or replace function sedona.ST_DistanceSphere (geomA BINARY, geomB BINARY, radius DOUBLE)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_DistanceSphere'
;
create or replace function sedona.ST_DistanceSpheroid (geomA BINARY, geomB BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_DistanceSpheroid'
;
create or replace function sedona.ST_FrechetDistance (geomA BINARY, geomB BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_FrechetDistance'
;
create or replace function sedona.ST_LengthSpheroid (geom BINARY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_LengthSpheroid'
;
create or replace function sedona.ST_GeometricMedian (geom BINARY, tolerance FLOAT, maxIter NUMBER, failIfNotConverged BOOLEAN)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeometricMedian'
;
create or replace function sedona.ST_GeometricMedian (geom BINARY, tolerance FLOAT)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeometricMedian'
;
create or replace function sedona.ST_GeometricMedian (geom BINARY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeometricMedian'
;
create or replace function sedona.ST_GeometricMedian (geom BINARY, tolerance FLOAT, maxIter NUMBER)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_GeometricMedian'
;
create or replace function sedona.ST_NumPoints (geom BINARY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_NumPoints'
;
create or replace function sedona.ST_Translate (geom BINARY, deltaX DOUBLE, deltaY DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Translate'
;
create or replace function sedona.ST_Translate (geom BINARY, deltaX DOUBLE, deltaY DOUBLE, deltaZ DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_Translate'
;
create or replace function sedona.ST_SimplifyPreserveTopology (geometry BINARY, distanceTolerance DOUBLE)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFs.ST_SimplifyPreserveTopology'
;
create or replace function sedona.ST_Affine (geometry GEOMETRY, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE, f DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Affine'
;
create or replace function sedona.ST_Affine (geometry GEOMETRY, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE, f DOUBLE, g DOUBLE, h DOUBLE, i DOUBLE, xOff DOUBLE, yOff DOUBLE, zOff DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Affine'
;
create or replace function sedona.ST_Angle (geom1 GEOMETRY, geom2 GEOMETRY, geom3 GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Angle'
;
create or replace function sedona.ST_Angle (geom1 GEOMETRY, geom2 GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Angle'
;
create or replace function sedona.ST_Angle (geom1 GEOMETRY, geom2 GEOMETRY, geom3 GEOMETRY, geom4 GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Angle'
;
create or replace function sedona.ST_Area (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Area'
;
create or replace function sedona.ST_AsEWKB (geometry GEOMETRY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsEWKB'
;
create or replace function sedona.ST_AsEWKT (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsEWKT'
;
create or replace function sedona.ST_AsGML (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsGML'
;
create or replace function sedona.ST_AsKML (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsKML'
;
create or replace function sedona.ST_Azimuth (left GEOMETRY, right GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Azimuth'
;
create or replace function sedona.ST_Buffer (geometry GEOMETRY, radius DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Buffer'
;
create or replace function sedona.ST_Covers (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Covers'
;
create or replace function sedona.ST_Crosses (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Crosses'
;
create or replace function sedona.ST_Equals (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Equals'
;
create or replace function sedona.ST_Force2D (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Force2D'
;
create or replace function sedona.ST_GeoHash (geometry GEOMETRY, precision NUMBER)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_GeoHash'
;
create or replace function sedona.ST_IsEmpty (geometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsEmpty'
;
create or replace function sedona.ST_IsRing (geometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsRing'
;
create or replace function sedona.ST_IsValid (geometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsValid'
;
create or replace function sedona.ST_IsValid (geometry GEOMETRY, flags NUMBER)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsValid'
;
create or replace function sedona.ST_Length (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Length'
;
create or replace function sedona.ST_Multi (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Multi'
;
create or replace function sedona.ST_NDims (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_NDims'
;
create or replace function sedona.ST_NPoints (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_NPoints'
;
create or replace function sedona.ST_PointN (geometry GEOMETRY, n NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_PointN'
;
create or replace function sedona.ST_Polygon (geometry GEOMETRY, srid NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Polygon'
;
create or replace function sedona.ST_Reverse (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Reverse'
;
create or replace function sedona.ST_SRID (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_SRID'
;
create or replace function sedona.ST_AsText (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsText'
;
create or replace function sedona.ST_SetSRID (geometry GEOMETRY, srid NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_SetSRID'
;
create or replace function sedona.ST_Split (input GEOMETRY, blade GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Split'
;
create or replace function sedona.ST_Touches (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Touches'
;
create or replace function sedona.ST_Union (leftGeom GEOMETRY, rightGeom GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Union'
;
create or replace function sedona.ST_Within (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Within'
;
create or replace function sedona.ST_X (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_X'
;
create or replace function sedona.ST_XMax (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_XMax'
;
create or replace function sedona.ST_XMin (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_XMin'
;
create or replace function sedona.ST_Y (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Y'
;
create or replace function sedona.ST_YMax (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_YMax'
;
create or replace function sedona.ST_YMin (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_YMin'
;
create or replace function sedona.ST_Z (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Z'
;
create or replace function sedona.ST_ZMax (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ZMax'
;
create or replace function sedona.ST_ZMin (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ZMin'
;
create or replace function sedona.ST_DWithin (geomA GEOMETRY, geomB GEOMETRY, distance DOUBLE)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_DWithin'
;
create or replace function sedona.ST_Force3D (geom GEOMETRY, zValue DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Force3D'
;
create or replace function sedona.ST_Force3D (geom GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Force3D'
;
create or replace function sedona.ST_NRings (geom GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_NRings'
;
create or replace function sedona.GeometryType (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.GeometryType'
;
create or replace function sedona.ST_AddPoint (linestring GEOMETRY, point GEOMETRY, position NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AddPoint'
;
create or replace function sedona.ST_AsBinary (geometry GEOMETRY)
returns BINARY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsBinary'
;
create or replace function sedona.ST_AsGeoJSON (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AsGeoJSON'
;
create or replace function sedona.ST_Boundary (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Boundary'
;
create or replace function sedona.ST_BoundingDiagonal (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_BoundingDiagonal'
;
create or replace function sedona.ST_BuildArea (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_BuildArea'
;
create or replace function sedona.ST_Centroid (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Centroid'
;
create or replace function sedona.ST_ClosestPoint (geometry1 GEOMETRY, geometry2 GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ClosestPoint'
;
create or replace function sedona.ST_CollectionExtract (geometry GEOMETRY, geomType NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_CollectionExtract'
;
create or replace function sedona.ST_CollectionExtract (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_CollectionExtract'
;
create or replace function sedona.ST_ConcaveHull (geometry GEOMETRY, pctConvex DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ConcaveHull'
;
create or replace function sedona.ST_ConcaveHull (geometry GEOMETRY, pctConvex DOUBLE, allowHoles BOOLEAN)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ConcaveHull'
;
create or replace function sedona.ST_Contains (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Contains'
;
create or replace function sedona.ST_CoordDim (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_CoordDim'
;
create or replace function sedona.ST_ConvexHull (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ConvexHull'
;
create or replace function sedona.ST_CoveredBy (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_CoveredBy'
;
create or replace function sedona.ST_Difference (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Difference'
;
create or replace function sedona.ST_Dimension (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Dimension'
;
create or replace function sedona.ST_Disjoint (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Disjoint'
;
create or replace function sedona.ST_Distance (left GEOMETRY, right GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Distance'
;
create or replace function sedona.ST_3DDistance (left GEOMETRY, right GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_3DDistance'
;
create or replace function sedona.ST_DumpPoints (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_DumpPoints'
;
create or replace function sedona.ST_EndPoint (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_EndPoint'
;
create or replace function sedona.ST_Envelope (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Envelope'
;
create or replace function sedona.ST_ExteriorRing (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ExteriorRing'
;
create or replace function sedona.ST_FlipCoordinates (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_FlipCoordinates'
;
create or replace function sedona.ST_Force_2D (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Force_2D'
;
create or replace function sedona.ST_GeometryN (geometry GEOMETRY, n NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_GeometryN'
;
create or replace function sedona.ST_GeometryType (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_GeometryType'
;
create or replace function sedona.ST_HausdorffDistance (geom1 GEOMETRY, geom2 GEOMETRY, densifyFrac DOUBLE)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_HausdorffDistance'
;
create or replace function sedona.ST_HausdorffDistance (geom1 GEOMETRY, geom2 GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_HausdorffDistance'
;
create or replace function sedona.ST_InteriorRingN (geometry GEOMETRY, n NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_InteriorRingN'
;
create or replace function sedona.ST_Intersection (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Intersection'
;
create or replace function sedona.ST_Intersects (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Intersects'
;
create or replace function sedona.ST_IsClosed (geometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsClosed'
;
create or replace function sedona.ST_IsCollection (geometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsCollection'
;
create or replace function sedona.ST_IsSimple (geometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsSimple'
;
create or replace function sedona.ST_IsValidReason (geometry GEOMETRY)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsValidReason'
;
create or replace function sedona.ST_IsValidReason (geometry GEOMETRY, flags NUMBER)
returns VARCHAR
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_IsValidReason'
;
create or replace function sedona.ST_LineFromMultiPoint (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_LineFromMultiPoint'
;
create or replace function sedona.ST_LineInterpolatePoint (geom GEOMETRY, fraction DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_LineInterpolatePoint'
;
create or replace function sedona.ST_LineLocatePoint (geom GEOMETRY, point GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_LineLocatePoint'
;
create or replace function sedona.ST_LineMerge (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_LineMerge'
;
create or replace function sedona.ST_LineSubstring (geom GEOMETRY, fromFraction DOUBLE, toFraction DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_LineSubstring'
;
create or replace function sedona.ST_MakeLine (geometryCollection GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MakeLine'
;
create or replace function sedona.ST_MakeLine (point1 GEOMETRY, point2 GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MakeLine'
;
create or replace function sedona.ST_MakePolygon (shell GEOMETRY, holes GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MakePolygon'
;
create or replace function sedona.ST_MakePolygon (shell GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MakePolygon'
;
create or replace function sedona.ST_MakeValid (geometry GEOMETRY, keepCollapsed BOOLEAN)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MakeValid'
;
create or replace function sedona.ST_MakeValid (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MakeValid'
;
create or replace function sedona.ST_MinimumBoundingCircle (geometry GEOMETRY, quadrantSegments NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_MinimumBoundingCircle'
;
create or replace function sedona.ST_Normalize (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Normalize'
;
create or replace function sedona.ST_NumGeometries (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_NumGeometries'
;
create or replace function sedona.ST_NumInteriorRings (geometry GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_NumInteriorRings'
;
create or replace function sedona.ST_OrderingEquals (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_OrderingEquals'
;
create or replace function sedona.ST_Overlaps (leftGeometry GEOMETRY, rightGeometry GEOMETRY)
returns BOOLEAN
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Overlaps'
;
create or replace function sedona.ST_PointOnSurface (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_PointOnSurface'
;
create or replace function sedona.ST_PrecisionReduce (geometry GEOMETRY, precisionScale NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_PrecisionReduce'
;
create or replace function sedona.ST_ReducePrecision (geometry GEOMETRY, precisionScale NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_ReducePrecision'
;
create or replace function sedona.ST_RemovePoint (linestring GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_RemovePoint'
;
create or replace function sedona.ST_RemovePoint (linestring GEOMETRY, position NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_RemovePoint'
;
create or replace function sedona.ST_S2CellIDs (input GEOMETRY, level NUMBER)
returns ARRAY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_S2CellIDs'
;
create or replace function sedona.ST_SetPoint (linestring GEOMETRY, position NUMBER, point GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_SetPoint'
;
create or replace function sedona.ST_StartPoint (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_StartPoint'
;
create or replace function sedona.ST_SubDivide (geometry GEOMETRY, maxVertices NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_SubDivide'
;
create or replace function sedona.ST_SymDifference (leftGeom GEOMETRY, rightGeom GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_SymDifference'
;
create or replace function sedona.ST_Transform (geometry GEOMETRY, sourceCRS VARCHAR, targetCRS VARCHAR, lenient BOOLEAN)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Transform'
;
create or replace function sedona.ST_Transform (geometry GEOMETRY, sourceCRS VARCHAR, targetCRS VARCHAR)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Transform'
;
create or replace function sedona.ST_VoronoiPolygons (geometry GEOMETRY, tolerance DOUBLE, extent GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_VoronoiPolygons'
;
create or replace function sedona.ST_VoronoiPolygons (geometry GEOMETRY, tolerance DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_VoronoiPolygons'
;
create or replace function sedona.ST_VoronoiPolygons (geometry GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_VoronoiPolygons'
;
create or replace function sedona.ST_AreaSpheroid (geometry GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_AreaSpheroid'
;
create or replace function sedona.ST_DistanceSphere (geomA GEOMETRY, geomB GEOMETRY, radius DOUBLE)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_DistanceSphere'
;
create or replace function sedona.ST_DistanceSphere (geomA GEOMETRY, geomB GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_DistanceSphere'
;
create or replace function sedona.ST_DistanceSpheroid (geomA GEOMETRY, geomB GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_DistanceSpheroid'
;
create or replace function sedona.ST_FrechetDistance (geomA GEOMETRY, geomB GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_FrechetDistance'
;
create or replace function sedona.ST_LengthSpheroid (geom GEOMETRY)
returns DOUBLE
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_LengthSpheroid'
;
create or replace function sedona.ST_GeometricMedian (geom GEOMETRY, tolerance FLOAT)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_GeometricMedian'
;
create or replace function sedona.ST_GeometricMedian (geom GEOMETRY)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_GeometricMedian'
;
create or replace function sedona.ST_GeometricMedian (geom GEOMETRY, tolerance FLOAT, maxIter NUMBER)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_GeometricMedian'
;
create or replace function sedona.ST_NumPoints (geom GEOMETRY)
returns NUMBER
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_NumPoints'
;
create or replace function sedona.ST_Translate (geom GEOMETRY, deltaX DOUBLE, deltaY DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Translate'
;
create or replace function sedona.ST_Translate (geom GEOMETRY, deltaX DOUBLE, deltaY DOUBLE, deltaZ DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_Translate'
;
create or replace function sedona.ST_SimplifyPreserveTopology (geometry GEOMETRY, distanceTolerance DOUBLE)
returns GEOMETRY
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.UDFsV2.ST_SimplifyPreserveTopology'
;
-- UDTFs --
create or replace function sedona.ST_MinimumBoundingRadius (geom BINARY)
returns table(center BINARY, radius DOUBLE)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_MinimumBoundingRadius'
;
create or replace function sedona.ST_InterSection_Aggr (geom BINARY)
returns table(intersected BINARY)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_Intersection_Aggr'
;
create or replace function sedona.ST_SubDivideExplode (geom BINARY, maxVertices NUMBER)
returns table(geom BINARY)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_SubDivideExplode'
;
create or replace function sedona.ST_Envelope_Aggr (geom BINARY)
returns table(envelope BINARY)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_Envelope_Aggr'
;
create or replace function sedona.ST_Union_Aggr (geom BINARY)
returns table(unioned BINARY)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_Union_Aggr'
;
create or replace function sedona.ST_Collect (geom BINARY)
returns table(collection BINARY)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_Collect'
;
create or replace function sedona.ST_Dump (geomCollection BINARY)
returns table(geom BINARY)
language java
RETURNS NULL ON NULL INPUT
IMMUTABLE
imports = ('@ApacheSedona/sedona-snowflake-1.5.1.jar', '@ApacheSedona/geotools-wrapper-1.5.1-28.2.jar')
handler = 'org.apache.sedona.snowflake.snowsql.udtfs.ST_Dump'
;
