## ST_Distance

Introduction: Return the Euclidean distance between A and B

Format: `ST_Distance (A:geometry, B:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Distance(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```

## ST_ConvexHull

Introduction: Return the Convex Hull of polgyon A

Format: `ST_ConvexHull (A:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_ConvexHull(polygondf.countyshape)
FROM polygondf
```

## ST_Envelope

Introduction: Return the envelop boundary of A

Format: `ST_Envelope (A:geometry)`

Since: `v1.0.0`

Spark SQL example:
```
SELECT ST_Envelope(polygondf.countyshape)
FROM polygondf
```

## ST_Length

Introduction: Return the perimeter of A

Format: ST_Length (A:geometry)

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Length(polygondf.countyshape)
FROM polygondf
```

## ST_Area

Introduction: Return the area of A

Format: `ST_Area (A:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Area(polygondf.countyshape)
FROM polygondf
```

## ST_Centroid

Introduction: Return the centroid point of A

Format: `ST_Centroid (A:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Centroid(polygondf.countyshape)
FROM polygondf
```



## ST_Transform

Introduction:

Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS

!!!note
	By default, ==ST_Transform== assumes Longitude/Latitude is your coordinate X/Y. If this is not the case, set UseLongitudeLatitudeOrder as "false".

!!!note
	If ==ST_Transform== throws an Exception called "Bursa wolf parameters required", you need to disable the error notification in ST_Transform. You can append a boolean value at the end.

Format: `ST_Transform (A:geometry, SourceCRS:string, TargetCRS:string, [Optional] UseLongitudeLatitudeOrder:Boolean, [Optional] DisableError)`

Since: `v1.0.0`

Spark SQL example (simple):
```SQL
SELECT ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857') 
FROM polygondf
```

Spark SQL example (with optional parameters):
```SQL
SELECT ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false)
FROM polygondf
```

!!!note
	The detailed EPSG information can be searched on [EPSG.io](https://epsg.io/).

## ST_Intersection

Introduction: Return the intersection geometry of A and B

Format: `ST_Intersection (A:geometry, B:geometry)`

Since: `v1.1.0`

Spark SQL example:

```SQL
SELECT ST_Intersection(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```
