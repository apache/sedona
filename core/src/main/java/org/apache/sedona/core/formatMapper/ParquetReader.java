package org.apache.sedona.core.formatMapper;

import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.formatMapper.parquet.ParquetFormatMapper;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.io.parquet.ParquetFileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.util.List;

public class ParquetReader extends RddReader {
    public static <T extends Geometry> SpatialRDD<T> createSpatialRDD(JavaRDD rawRDD,
                                                                      ParquetFormatMapper<T> formatMapper,
                                                                      GeometryType geometryType) {
        SpatialRDD spatialRDD = new SpatialRDD<T>(geometryType);
        spatialRDD.rawSpatialRDD = rawRDD.mapPartitions(formatMapper);
        return spatialRDD;
    }
    
    public static <T extends Geometry> SpatialRDD<T> readToGeometryRDD(JavaSparkContext sc,
                                                                       String inputPath,
                                                                       GeometryType geometryType,
                                                                       String geometryColumn,
                                                                       List<String> userColumns) throws IOException {
        JavaRDD<GenericRecord> recordJavaRDD = ParquetFileReader.readFile(sc, geometryColumn, userColumns, inputPath);
        ParquetFormatMapper<T> formatMapper =
                new ParquetFormatMapper<T>(geometryType, geometryColumn, userColumns);
        return createSpatialRDD(recordJavaRDD, formatMapper, geometryType);
    }
}
