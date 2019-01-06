package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

public class GeometryReader {
    private static SpatialRDD<Geometry> readToGeometryRDDfromFile(JavaSparkContext sc, String inputPath,
                                                                  FileDataSplitter fileFormat, boolean allowTopologicallyInvalidGeometries,
                                                                  boolean skipInvalidSyntaxGeometries) {
        JavaRDD rawTextRDD = sc.textFile(inputPath);
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(fileFormat, true);
        formatMapper.allowTopologicallyInvalidGeometries = allowTopologicallyInvalidGeometries;
        formatMapper.skipSyntacticallyInvalidGeometries = skipInvalidSyntaxGeometries;
        spatialRDD.rawSpatialRDD = rawTextRDD.mapPartitions(formatMapper);
        spatialRDD.fieldNames = formatMapper.readPropertyNames(rawTextRDD.take(1).get(0).toString(), fileFormat);
        return spatialRDD;
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath, FileDataSplitter fileFormat) {
        return readToGeometryRDDfromFile(sc, inputPath, fileFormat, true, false);
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath,
                                                         FileDataSplitter fileFormat, boolean allowTopologicallyInvalidGeometries) {
        return readToGeometryRDDfromFile(sc, inputPath,fileFormat, allowTopologicallyInvalidGeometries, false);
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath,
                                                         FileDataSplitter fileFormat, boolean allowTopologicallyInvalidGeometries,
                                                         boolean skipInvalidSyntaxGeometries) {
        return readToGeometryRDDfromFile(sc, inputPath,fileFormat, allowTopologicallyInvalidGeometries, skipInvalidSyntaxGeometries);
    }
}
