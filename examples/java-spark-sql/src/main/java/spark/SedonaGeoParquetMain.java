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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class SedonaGeoParquetMain {

    protected static Properties properties;
    protected static String parquetPath;
    protected static SedonaSparkSession session;

    public static void main(String args[]) {

        session = new SedonaSparkSession();
        //Get parquetPath and any other application.properties
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Properties properties = new Properties();
            InputStream is = loader.getResourceAsStream("application.properties");
            properties.load(is);
            parquetPath = properties.getProperty("parquet.path");
        } catch (IOException e) {
            e.printStackTrace();
            parquetPath = "";
        }
        GeoParquetAccessor accessor = new GeoParquetAccessor(session.session, parquetPath);
        //Test parquet happens to be in New Zealand Transverse Mercator (EPSG:2193) (meters)
        List<Geometry> geoms = accessor.selectFeaturesByPolygon(1155850, 4819840, 1252000, 4748100, "geometry");
        System.out.println("Coordinates of convex hull of points in boundary:");
        for (Geometry geom : geoms) {
            Coordinate[] convexHullCoordinates = geom.convexHull().getCoordinates();
            for (Coordinate coord : convexHullCoordinates) {
                System.out.println(String.format("\t%s", coord.toString()));
            }
        }
    }
}
