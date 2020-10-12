/*
 * FILE: GeoJsonReader
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.formatMapper;

import org.locationtech.jts.geom.Geometry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

public class GeoJsonReader extends RddReader
{

    /**
     * Read a SpatialRDD from a file.
     * @param sc
     * @param inputPath
     * @return
     */
    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath) {
        return readToGeometryRDD(sc, inputPath, true, false);
    }

    /**
     * Read a SpatialRDD from a file.
     * @param sc
     * @param inputPath
     * @param allowInvalidGeometries whether allows topology-invalid geometries exist in the generated RDD
     * @param skipSyntacticallyInvalidGeometries whether allows GeoSpark to automatically skip syntax-invalid geometries, rather than throw errors
     * @return
     */
    public static SpatialRDD<Geometry> readToGeometryRDD(JavaSparkContext sc, String inputPath, boolean allowInvalidGeometries, boolean skipSyntacticallyInvalidGeometries) {
        JavaRDD rawTextRDD = sc.textFile(inputPath);
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(FileDataSplitter.GEOJSON, true);
        formatMapper.allowTopologicallyInvalidGeometries = allowInvalidGeometries;
        formatMapper.skipSyntacticallyInvalidGeometries = skipSyntacticallyInvalidGeometries;
        return createSpatialRDD(rawTextRDD, formatMapper);
    }

    /**
     * Read a SpatialRDD from a string type rdd.
     * @param rawTextRDD a string type RDD
     * @return
     */
    public static SpatialRDD<Geometry> readToGeometryRDD(JavaRDD rawTextRDD) {
        return readToGeometryRDD(rawTextRDD, true, false);
    }

    /**
     * Read a SpatialRDD from a string type rdd.
     * @param rawTextRDD a string type RDD
     * @param allowInvalidGeometries whether allows topology-invalid geometries exist in the generated RDD
     * @param skipSyntacticallyInvalidGeometries whether allows GeoSpark to automatically skip syntax-invalid geometries, rather than throw errors
     * @return
     */
    public static SpatialRDD<Geometry> readToGeometryRDD(JavaRDD rawTextRDD, boolean allowInvalidGeometries, boolean skipSyntacticallyInvalidGeometries) {
        FormatMapper<Geometry> formatMapper = new FormatMapper<Geometry>(FileDataSplitter.GEOJSON, true);
        formatMapper.allowTopologicallyInvalidGeometries = allowInvalidGeometries;
        formatMapper.skipSyntacticallyInvalidGeometries = skipSyntacticallyInvalidGeometries;
        return createSpatialRDD(rawTextRDD, formatMapper);
    }
}
