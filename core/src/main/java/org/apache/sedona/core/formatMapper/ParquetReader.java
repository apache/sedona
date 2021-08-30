package org.apache.sedona.core.formatMapper;

import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.formatMapper.parquet.ParquetFormatMapper;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.io.parquet.ParquetFileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Parquet Reader Class
 */
public class ParquetReader extends RddReader {
    private static <T extends Geometry> SpatialRDD<T> createSpatialRDD(JavaRDD rawRDD,
                                                                      ParquetFormatMapper<T> formatMapper) {
        SpatialRDD spatialRDD = new SpatialRDD<T>();
        spatialRDD.rawSpatialRDD = rawRDD.mapPartitions(formatMapper);
        return spatialRDD;
    }
    
    /**
     *
     * Converts a Parquet File into Geometry RDD
     * @param sc Spark Context
     * @param inputPath Parquet files input
     * @param geometryColumn Geometry Column in the Parquet File
     * @param userColumns Relevant User Columns to be Read from the Parquet File
     * @param defaultGeometryType Default Geometry Type to deserialize Geometry stored as Array Types
     * @param <T>
     * @return Geometry RDD
     * @throws IOException
     */
    public static <T extends Geometry> SpatialRDD<T> readToGeometryRDD(JavaSparkContext sc,
                                                                       List<String> inputPath,
                                                                       String geometryColumn,
                                                                       List<String> userColumns,
                                                                       GeometryType defaultGeometryType) throws SedonaException {
        JavaRDD<GenericRecord> recordJavaRDD = ParquetFileReader.readFile(sc, geometryColumn, userColumns, inputPath.toArray(new String[inputPath.size()]));
        ParquetFormatMapper<T> formatMapper =
                new ParquetFormatMapper<T>(geometryColumn, userColumns,defaultGeometryType);
        return createSpatialRDD(recordJavaRDD, formatMapper);
    }
    
    /**
     * Converts a Parquet File into Geometry RDD
     * @param sc Spark Context
     * @param inputPath Parquet files input
     * @param geometryColumn Geometry Column in the Parquet File
     * @param userColumns Relevant User Columns to be Read from the Parquet File
     * @param <T>
     * @return Geometry RDD
     * @throws IOException
     */
    public static <T extends Geometry> SpatialRDD<T> readToGeometryRDD(JavaSparkContext sc,
                                                                       List<String> inputPath,
                                                                       String geometryColumn,
                                                                       List<String> userColumns) throws SedonaException {
        return readToGeometryRDD(sc,inputPath,geometryColumn,userColumns,null);
    }
}
