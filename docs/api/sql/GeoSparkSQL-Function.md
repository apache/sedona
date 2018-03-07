### ST_Distance (A:geometry, B:geometry)

Introduction:

*Return the Euclidean distance between A and B*

Since: v1.0.0

Spark SQL example:
```
select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf
```

### ST_ConvexHull (A:geometry)

Introduction:

*Return the Convex Hull of polgyon A*

Since: v1.0.0

Spark SQL example:
```
select ST_ConvexHull(polygondf.countyshape) from polygondf
```

### ST_Envelope (A:geometry)

Introduction:

*Return the envelop boundary of A*

Since: v1.0.0

Spark SQL example:
```
select ST_Envelope(polygondf.countyshape) from polygondf
```

### ST_Length (A:geometry)

Introduction:

*Return the perimeter of A*

Spark SQL example:
```
select ST_Length(polygondf.countyshape) from polygondf
```

### ST_Area (A:geometry)

Introduction:

*Return the area of A*

Since: v1.0.0

Spark SQL example:
```
select ST_Area(polygondf.countyshape) from polygondf
```

### ST_Centroid (A:geometry)

Introduction:

*Return the centroid point of A*

Spark SQL example:
```
select ST_Centroid(polygondf.countyshape) from polygondf
```



### ST_Transform (A:geometry, SourceCRS:string, TargetCRS:string, [Optional] UseLongitudeLatitudeOrder:Boolean, [Optional] DisableError)

Introduction:

*Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS*

By default, ST_Transform assumes Longitude/Latitude is your coordinate X/Y. If this is not the case, set UseLongitudeLatitudeOrder as "false".

If your ST_Transform throws an Exception called "Bursa wolf parameters required", you need to disable the error notification in ST_Transform. You can append a boolean value at the end.

Since: v1.0.0

Spark SQL example (simple):
```
select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false) from polygondf
```

Spark SQL example (with optional parameters):
```
select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false) from polygondf
```

### ST_Intersection (A:geometry, B:geometry)

Introduction:

*Return the intersection geometry of A and B*

Since: v1.1.0

Spark SQL example:

```
select ST_Intersection(polygondf.countyshape, polygondf.countyshape) from polygondf
```
