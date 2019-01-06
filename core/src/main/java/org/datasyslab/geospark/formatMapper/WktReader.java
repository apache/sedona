package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

public class WktReader {
    private static SpatialRDD<Geometry> readToGeometryRDDfromFile(JavaSparkContext sc, String inputPath, boolean allowTopologicallyInvalidGeometries) {
        JavaRDD rawTextRDD = sc.textFile(inputPath);
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(FileDataSplitter.WKT, true);
        formatMapper.allowTopologicallyInvalidGeometries = allowTopologicallyInvalidGeometries;
        spatialRDD.rawSpatialRDD = rawTextRDD.mapPartitions(formatMapper);
        return spatialRDD;
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath) {
        return readToGeometryRDDfromFile(sc, inputPath, true);
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath, boolean allowTopologicallyInvalidGeometries) {
        return readToGeometryRDDfromFile(sc, inputPath, allowTopologicallyInvalidGeometries);
    }
}
