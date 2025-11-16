/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package spark;

import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.util.List;


public class GeoParquetAccessor {

    private final SparkSession session;
    private String parquetPath;

    public GeoParquetAccessor() {
        this.session = new SedonaSparkSession().getSession();
        this.parquetPath = "";
    }

    //Overload with constructor that has Spark session provided
    //Use to avoid error - can't have two SparkContext objects on one JVM
    public GeoParquetAccessor(SparkSession session, String parquetPath) {
        this.session = session;
        this.parquetPath = parquetPath;
    }

    public List<Geometry> selectFeaturesByPolygon(double xmin, double ymax,
                                                  double xmax, double ymin,
                                                  String geometryColumn) {

        //Read the GeoParquet file into a DataFrame
        Dataset<Row> insarDF = session.read().format("geoparquet").load(parquetPath);

        //Convert the DataFrame to a SpatialRDD
        //The second argument to toSpatialRdd is the name of the geometry column.
        SpatialRDD<Geometry> insarRDD = Adapter.toSpatialRdd(insarDF, geometryColumn);

        // Define the polygon for the spatial query
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[] {
            new Coordinate(xmin, ymin),
            new Coordinate(xmax, ymin),
            new Coordinate(xmax, ymax),
            new Coordinate(xmin, ymax),
            new Coordinate(xmin, ymin) // A closed polygon has the same start and end coordinate
        };
        Polygon queryPolygon = geometryFactory.createPolygon(coordinates);

        // Perform the spatial range query
        // This will return all geometries that intersect with the query polygon.
        // Alternatives are SpatialPredicate.CONTAINS or SpatialPredicate.WITHIN
        SpatialRDD<Geometry> resultRDD = new SpatialRDD<>();
        try {
            resultRDD.rawSpatialRDD = RangeQuery.SpatialRangeQuery(insarRDD, queryPolygon, SpatialPredicate.INTERSECTS, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Collect the results back to the driver
        return resultRDD.getRawSpatialRDD().collect();
    }

}
