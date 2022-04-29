/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.formatMapper.FormatUtils;
import org.apache.spark.sql.sedona_sql.expressions.geohash.GeoHashDecoder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

public class Constructors {
    public static class ST_PointFromText extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String s, @DataTypeHint("String") String inputDelimiter) throws ParseException {
            FileDataSplitter delimiter = inputDelimiter == null? FileDataSplitter.CSV:FileDataSplitter.getFileDataSplitter(inputDelimiter);
            FormatUtils<Geometry> formatUtils = new FormatUtils(delimiter, false, GeometryType.POINT);
            return formatUtils.readGeometry(s);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String s) throws ParseException {
            return eval(s, null);
        }
    }

    public static class ST_PolygonFromText extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String s, @DataTypeHint("String") String inputDelimiter) throws ParseException {
            // The default delimiter is comma. Otherwise, use the delimiter given by the user
            FileDataSplitter delimiter = inputDelimiter == null? FileDataSplitter.CSV:FileDataSplitter.getFileDataSplitter(inputDelimiter);
            FormatUtils<Geometry> formatUtils = new FormatUtils(delimiter, false, GeometryType.POLYGON);
            return formatUtils.readGeometry(s);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String s) throws ParseException {
            return eval(s, null);
        }
    }

    public static class ST_PolygonFromEnvelope extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("Double") Double minX, @DataTypeHint("Double") Double minY,
                             @DataTypeHint("Double") Double maxX, @DataTypeHint("Double") Double maxY) {
            Coordinate[] coordinates = new Coordinate[5];
            coordinates[0] = new Coordinate(minX, minY);
            coordinates[1] = new Coordinate(minX, maxY);
            coordinates[2] = new Coordinate(maxX, maxY);
            coordinates[3] = new Coordinate(maxX, minY);
            coordinates[4] = coordinates[0];
            GeometryFactory geometryFactory = new GeometryFactory();
            return geometryFactory.createPolygon(coordinates);
        }
    }

    public static class ST_GeomFromWKT extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String wktString) throws ParseException {
            FormatUtils formatUtils = new FormatUtils(FileDataSplitter.WKT, false);
            return formatUtils.readGeometry(wktString);
        }
    }

    public static class ST_GeomFromWKB extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String wkbString) throws ParseException {
            FormatUtils formatUtils = new FormatUtils(FileDataSplitter.WKB, false);
            return formatUtils.readGeometry(wkbString);
        }
    }

    public static class ST_GeomFromGeoJSON extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String geoJson) throws ParseException {
            FormatUtils formatUtils = new FormatUtils(FileDataSplitter.GEOJSON, false);
            return formatUtils.readGeometry(geoJson);
        }
    }

    public static class ST_GeomFromGeoHash extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String value,
                             @DataTypeHint("Int") Integer precision) throws ParseException {
            // The default precision is the geohash length. Otherwise, use the precision given by the user
            scala.Option<Object> optionPrecision = scala.Option.apply(precision);
            return GeoHashDecoder.decode(value, optionPrecision);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint("String") String value) throws ParseException {
            return eval(value, null);
        }
    }
}