/**
 * FILE: GeoJsonReader
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 * <p>
 * MIT License
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

public class GeoJsonReader {

    private static SpatialRDD<Geometry> readToGeometryRDDfromFile(JavaSparkContext sc, String inputPath, boolean allowInvalidGeometries) {
        JavaRDD rawTextRDD = sc.textFile(inputPath);
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(FileDataSplitter.GEOJSON, true);
        formatMapper.allowInvalidGeometries = allowInvalidGeometries;
        spatialRDD.rawSpatialRDD = rawTextRDD.mapPartitions(formatMapper);
        spatialRDD.fieldNames = FormatMapper.readGeoJsonPropertyNames(rawTextRDD.take(1).get(0).toString());
        return spatialRDD;
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath) {
        return readToGeometryRDDfromFile(sc, inputPath, true);
    }

    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath, boolean allowInvalidGeometries) {
        return readToGeometryRDDfromFile(sc, inputPath, allowInvalidGeometries);
    }
}
