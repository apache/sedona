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
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class Functions {
    public static class ST_Area extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.area(geom);
        }
    }

    public static class ST_Azimuth extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.Functions.azimuth(geom1, geom2);
        }
    }

    public static class ST_Boundary extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.boundary(geom);
        }
    }

    public static class ST_Buffer extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
                Object o, @DataTypeHint("Double") Double radius) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.buffer(geom, radius);
        }
    }

    public static class ST_ConcaveHull extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double pctConvex) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.concaveHull(geom, pctConvex, false);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double pctConvex, @DataTypeHint("Boolean") Boolean allowHoles) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.concaveHull(geom, pctConvex, allowHoles);
        }
    }

    public static class ST_Envelope extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.envelope(geom);
        }
    }

    public static class ST_Distance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.Functions.distance(geom1, geom2);
        }
    }

    public static class ST_3DDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.Functions.distance3d(geom1, geom2);
        }
    }

    public static class ST_Length extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.length(geom);
        }
    }

    public static class ST_YMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.yMin(geom);
        }
    }

    public static class ST_YMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.yMax(geom);
        }
    }

    public static class ST_ZMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.zMax(geom);
        }
    }

    public static class ST_ZMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.zMin(geom);
        }
    }

    public static class ST_NDims extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.nDims(geom);
        }
    }

    public static class ST_Transform extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS)
            throws FactoryException, TransformException {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.transform(geom, sourceCRS, targetCRS);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS, @DataTypeHint("Boolean") Boolean lenient)
                throws FactoryException, TransformException {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.transform(geom, sourceCRS, targetCRS, lenient);
        }

    }

    public static class ST_FlipCoordinates extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.flipCoordinates(geom);
        }
    }

    public static class ST_GeoHash extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object geometry, Integer precision) {
            Geometry geom = (Geometry) geometry;
            return org.apache.sedona.common.Functions.geohash(geom, precision);
        }
    }

    public static class ST_PointOnSurface extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.pointOnSurface(geom);
        }
    }

    public static class ST_Reverse extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.reverse(geom);
        }
    }

    public static class ST_GeometryN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.geometryN(geom, n);
        }
    }

    public static class ST_InteriorRingN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.interiorRingN(geom, n);
        }
    }

    public static class ST_PointN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.pointN(geom, n);
        }
    }

    public static class ST_NPoints extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.nPoints(geom);
        }
    }

    public static class ST_NumGeometries extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.numGeometries(geom);
        }
    }

    public static class ST_NumInteriorRings extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.numInteriorRings(geom);
        }
    }

    public static class ST_ExteriorRing extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.exteriorRing(geom);
        }
    }

    public static class ST_AsEWKT extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKT(geom);
        }
    }

    public static class ST_AsText extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKT(geom);
        }
    }

    public static class ST_AsEWKB extends ScalarFunction {
        @DataTypeHint("Bytes")
        public byte[] eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKB(geom);
        }
    }

    public static class ST_AsBinary extends ScalarFunction {
        @DataTypeHint("Bytes")
        public byte[] eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKB(geom);
        }
    }

    public static class ST_AsGeoJSON extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asGeoJson(geom);
        }
    }

    public static class ST_AsGML extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asGML(geom);
        }
    }

    public static class ST_AsKML extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asKML(geom);
        }
    }

    public static class ST_Force_2D extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.force2D(geom);
        }
    }

    public static class ST_IsEmpty extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isEmpty(geom);
        }
    }

    public static class ST_X extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.x(geom);
        }
    }

    public static class ST_Y extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.y(geom);
        }
    }

    public static class ST_Z extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.z(geom);
        }
    }

    public static class ST_XMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.xMax(geom);
        }
    }

    public static class ST_XMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.xMin(geom);
        }
    }

    public static class ST_BuildArea extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.buildArea(geom);
        }
    }

    public static class ST_SetSRID extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int srid) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.setSRID(geom, srid);
        }
    }

    public static class ST_SRID extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.getSRID(geom);
        }
    }

    public static class ST_IsClosed extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isClosed(geom);
        }
    }

    public static class ST_IsRing extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isRing(geom);
        }
    }

    public static class ST_IsSimple extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isSimple(geom);
        }
    }

    public static class ST_IsValid extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isValid(geom);
        }
    }

    public static class ST_Normalize extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.normalize(geom);
        }
    }

    public static class ST_AddPoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry linestring = (Geometry) o1;
            Geometry point = (Geometry) o2;
            return org.apache.sedona.common.Functions.addPoint(linestring, point);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2,
                             int position) {
            Geometry linestring = (Geometry) o1;
            Geometry point = (Geometry) o2;
            return org.apache.sedona.common.Functions.addPoint(linestring, point, position);
        }
    }

    public static class ST_RemovePoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.removePoint(geom);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int offset) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.removePoint(geom, offset);
        }
    }

    public static class ST_SetPoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, int position,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry linestring = (Geometry) o1;
            Geometry point = (Geometry) o2;
            return org.apache.sedona.common.Functions.setPoint(linestring, position, point);
        }
    }

    public static class ST_LineFromMultiPoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.lineFromMultiPoint(geom);
        }
    }

    public static class ST_Split extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry input = (Geometry) o1;
            Geometry blade = (Geometry) o2;
            return org.apache.sedona.common.Functions.split(input, blade);
        }
    }

    public static class ST_S2CellIDs extends ScalarFunction {
        @DataTypeHint(value = "ARRAY<BIGINT>")
        public Long[] eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("INT") Integer level) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.s2CellIDs(geom, level);
        }
    }
}
