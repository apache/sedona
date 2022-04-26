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
import org.apache.sedona.core.utils.GeomUtils;
import org.locationtech.jts.io.WKTWriter;
import org.apache.spark.sql.sedona_sql.expressions.geohash.GeometryGeoHashEncoder;
import org.apache.spark.sql.sedona_sql.expressions.geohash.PointGeoHashEncoder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import scala.Option;

import java.util.Optional;

import static org.locationtech.jts.geom.Coordinate.NULL_ORDINATE;

public class Functions {
    public static class ST_Buffer extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(Object o, @DataTypeHint("Double") Double radius) {
            Geometry geom = (Geometry) o;
            return geom.buffer(radius);
        }
    }

    public static class ST_Distance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.distance(geom2);
        }
    }

    public static class ST_YMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            Coordinate[] points= geom.getCoordinates();
            double min=Double.MAX_VALUE;
            for(int i=0;i<points.length;i++){
                min=Math.min(points[i].getY(),min);
            }
            return min;
        }
    }

    public static class ST_YMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            Coordinate[] points= geom.getCoordinates();
            double max=Double.MIN_VALUE;
            for(int i=0;i<points.length;i++){
                max=Math.max(points[i].getY(),max);
            }
            return max;
        }
    }


    public static class ST_Transform extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS) {
            Geometry geom = (Geometry) o;
            try {
                CoordinateReferenceSystem sourceCRScode = CRS.decode(sourceCRS);
                CoordinateReferenceSystem targetCRScode = CRS.decode(targetCRS);
                MathTransform transform = CRS.findMathTransform(sourceCRScode, targetCRScode);
                geom = JTS.transform(geom, transform);
            } catch (FactoryException | TransformException e) {
                e.printStackTrace();
            }
            return geom;
        }
    }

    public static class ST_FlipCoordinates extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            GeomUtils.flipCoordinates(geom);
            return geom;
        }
    }

    public static class ST_GeoHash extends ScalarFunction {
        @DataTypeHint("RAW")
        public Optional<String> eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object geometry, Integer precision) {
            Geometry geom = (Geometry) geometry;
            Option<String> geoHash = GeometryGeoHashEncoder.calculate(geom, precision);
            if (geoHash.isDefined()){
                return Optional.of(geoHash.get());
            }
            return Optional.empty();
        }
    }

    public static class ST_PointOnSurface extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            GeomUtils.getInteriorPoint(geom);
            return geom;
        }
    }

    public static class ST_Reverse extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return geom.reverse();
        }
    }

    public static class ST_PointN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            if(!(o instanceof LineString)) {
                return null;
            }
            LineString lineString = (LineString) o;
            return GeomUtils.getNthPoint(lineString, n);
        }
    }

    public static class ST_ExteriorRing extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geometry = (Geometry) o;
            return GeomUtils.getExteriorRing(geometry);
        }
    }

    public static class ST_AsEWKT extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return GeomUtils.getEWKT(geom);
        }
    }

    public static class ST_Force_2D extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return GeomUtils.get2dGeom(geom);
        }
    }

    public static class ST_IsEmpty extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return geom.isEmpty();
        }
    }

    public static class ST_XMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            Coordinate[] coord = geom.getCoordinates();
            double max = Double.MIN_VALUE;
            for (int i = 0; i < coord.length; i++) {
                if (coord[i].getX() > max) {
                    max = coord[i].getX();
                }
            }
            return max;
        }
    }

    public static class ST_XMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            Coordinate[] coord = geom.getCoordinates();
            double min = Double.MAX_VALUE;
            for(int i=0;i< coord.length;i++){
                if(coord[i].getX()<min){
                    min = coord[i].getX();
                }
            }
            return min;
        }
    }

    public static class ST_BuildArea extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return GeomUtils.buildArea(geom);
        }
    }
}
