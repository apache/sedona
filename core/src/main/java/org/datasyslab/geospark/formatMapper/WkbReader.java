/**
 * FILE: WkbReader
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
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

public class WkbReader extends RddReader
{
    /**
     * Read a SpatialRDD from a file.
     * @param sc
     * @param inputPath
     * @param wkbColumn The column which contains the wkt string. Start from 0.
     * @param allowInvalidGeometries whether allows topology-invalid geometries exist in the generated RDD
     * @param skipSyntacticallyInvalidGeometries whether allows GeoSpark to automatically skip syntax-invalid geometries, rather than throw errors
     * @return
     */
    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath, int wkbColumn, boolean allowInvalidGeometries, boolean skipSyntacticallyInvalidGeometries) {
        JavaRDD rawTextRDD = sc.textFile(inputPath);
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(wkbColumn, -1, FileDataSplitter.WKB, true, null);
        formatMapper.allowTopologicallyInvalidGeometries = allowInvalidGeometries;
        formatMapper.skipSyntacticallyInvalidGeometries = skipSyntacticallyInvalidGeometries;
        return createSpatialRDD(rawTextRDD, formatMapper);
    }

    /**
     * Read a SpatialRDD from a string type rdd.
     * @param rawTextRDD a string type RDD
     * @param wkbColumn The column which contains the wkt string. Start from 0.
     * @param allowInvalidGeometries whether allows topology-invalid geometries exist in the generated RDD
     * @param skipSyntacticallyInvalidGeometries whether allows GeoSpark to automatically skip syntax-invalid geometries, rather than throw errors
     * @return
     */
    public static SpatialRDD<Geometry> readToGeometryRDD(JavaRDD rawTextRDD, int wkbColumn, boolean allowInvalidGeometries, boolean skipSyntacticallyInvalidGeometries) {
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(wkbColumn, -1, FileDataSplitter.WKB, true, null);
        formatMapper.allowTopologicallyInvalidGeometries = allowInvalidGeometries;
        formatMapper.skipSyntacticallyInvalidGeometries = skipSyntacticallyInvalidGeometries;
        return createSpatialRDD(rawTextRDD, formatMapper);
    }
}
