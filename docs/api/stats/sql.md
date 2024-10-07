## Overview

Sedona's stats module provides Scala and Python functions for conducting geospatial
statistical analysis on dataframes with spatial columns.
The stats module is built on top of the core module and provides a set of functions
that can be used to perform spatial analysis on these dataframes. The stats module
is designed to be used with the core module and the viz module to provide a
complete set of geospatial analysis tools.

## Using DBSCAN

The DBSCAN function is provided at `org.apache.sedona.stats.DBSCAN.dbscan` in scala/java and `sedona.stats.dbscan.dbscan` in python.

The function annotates a dataframe with a cluster label for each data record using the DBSCAN algorithm.
The dataframe should contain at least one `GeometryType` column. Rows must be unique. If one
geometry column is present it will be used automatically. If two are present, the one named
'geometry' will be used. If more than one are present and none are named 'geometry', the
column name must be provided. The new column will be named 'cluster'.

### Parameters

names in parentheses are python variable names

- dataframe - dataframe to cluster. Must contain at least one GeometryType column
- epsilon - minimum distance parameter of DBSCAN algorithm
- minPts (min_pts) - minimum number of points parameter of DBSCAN algorithm
- geometry - name of the geometry column
- includeOutliers (include_outliers) - whether to include outliers in the output. Default is false
- useSpheroid (use_spheroid) - whether to use a cartesian or spheroidal distance calculation. Default is false

The output is the input DataFrame with the cluster label added to each row. Outlier will have a cluster value of -1 if included.
