package org.apache.sedona.core.io;

import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;

public interface GeoFileReader<T extends Geometry> {
    public SpatialRDD<T> readFile(JavaSparkContext sc, String... inputPaths) throws IOException;
}
