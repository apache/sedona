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
package org.apache.sedona.flink.confluent;

import java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.common.FunctionsGeoTools;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class Functions {
    public static class GeometryType extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometryTypeWithMeasured(geom);
        }
    }

    public static class ST_LabelPoint extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.labelPoint(geom);
        }


        public Geometry eval(byte[] ewkb, @DataTypeHint("Integer") Integer gridResolution) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.labelPoint(geom, gridResolution);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Integer") Integer gridResolution,
                @DataTypeHint("Double") Double goodnessThreshold) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.labelPoint(geom, gridResolution, goodnessThreshold);
        }
    }

    public static class ST_Area extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.area(geom);
        }
    }

    public static class ST_AreaSpheroid extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.sphere.Spheroid.area(geom);
        }
    }

    public static class ST_Azimuth extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.azimuth(geom1, geom2);
        }
    }

    public static class ST_Boundary extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.boundary(geom);
        }
    }

    public static class ST_Buffer extends ScalarFunction {

        public Geometry eval(byte[] ewkb, @DataTypeHint("Double") Double radius)
                throws FactoryException, TransformException {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.buffer(geom, radius);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double radius,
                @DataTypeHint("Boolean") Boolean useSpheroid)
                throws FactoryException, TransformException {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.buffer(geom, radius, useSpheroid);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double radius,
                @DataTypeHint("Boolean") Boolean useSpheroid,
                @DataTypeHint("String") String params)
                throws FactoryException, TransformException {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.buffer(geom, radius, useSpheroid, params);
        }
    }

    public static class ST_BestSRID extends ScalarFunction {
        @DataTypeHint("Integer")
        public int eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.bestSRID(geom);
        }
    }

    public static class ST_ShiftLongitude extends ScalarFunction {

        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.shiftLongitude(geom);
        }
    }

    public static class ST_ClosestPoint extends ScalarFunction {

        public Geometry eval(byte[] ewkb, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.closestPoint(geom1, geom2);
        }
    }

    public static class ST_Centroid extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.getCentroid(geom);
        }
    }

    public static class ST_Collect extends ScalarFunction {

        public Geometry eval(byte[] ewkb, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            Geometry[] geoms = new Geometry[]{geom1, geom2};
            return org.apache.sedona.common.Functions.createMultiGeometry(geoms);
        }


        public byte[] eval(@DataTypeHint(inputGroup = InputGroup.ANY) byte[][] ewkb) {
            Geometry[] geoms = Arrays.stream(ewkb)
                    .map(GeometrySerde::deserialize)
                    .toArray(Geometry[]::new);

            return GeometrySerde.serialize(
                    org.apache.sedona.common.Functions.createMultiGeometry(geoms)
            );
        }
    }

    public static class ST_CollectionExtract extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.collectionExtract(geom);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Integer") Integer geoType) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.collectionExtract(geom, geoType);
        }
    }

    public static class ST_ConcaveHull extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double pctConvex) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.concaveHull(geom, pctConvex, false);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double pctConvex,
                @DataTypeHint("Boolean") Boolean allowHoles) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.concaveHull(geom, pctConvex, allowHoles);
        }
    }

    public static class ST_ConvexHull extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.convexHull(geom);
        }
    }

    public static class ST_CrossesDateLine extends ScalarFunction {
        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_CrossesDateLine() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.crossesDateLine(geom);
        }
    }

    public static class ST_Envelope extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.envelope(geom);
        }
    }

    public static class ST_Expand extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double uniformDelta) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.expand(geom, uniformDelta);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double deltaX,
                @DataTypeHint(value = "Double") Double deltaY) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.expand(geom, deltaX, deltaY);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double deltaX,
                @DataTypeHint(value = "Double") Double deltaY,
                @DataTypeHint(value = "Double") Double deltaZ) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.expand(geom, deltaX, deltaY, deltaZ);
        }
    }

    public static class ST_Dimension extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.dimension(geom);
        }
    }

    public static class ST_Difference extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.difference(geom1, geom2);
        }
    }

    public static class ST_Distance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.distance(geom1, geom2);
        }
    }

    public static class ST_DistanceSphere extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.sphere.Haversine.distance(geom1, geom2);
        }

        @DataTypeHint("Double")
        public Double eval(byte[] ewkb1, byte[] ewkb2,
                @DataTypeHint("Double") Double radius) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.sphere.Haversine.distance(geom1, geom2, radius);
        }
    }

    public static class ST_DistanceSpheroid extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.sphere.Spheroid.distance(geom1, geom2);
        }
    }

    public static class ST_3DDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.distance3d(geom1, geom2);
        }
    }

    public static class ST_Dump extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(byte[] ewkb) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.dump(geom1);
        }
    }

    public static class ST_DumpPoints extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(byte[] ewkb) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.dumpPoints(geom1);
        }
    }

    public static class ST_EndPoint extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.endPoint(geom1);
        }
    }

    public static class ST_GeometryType extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometryType(geom);
        }
    }

    public static class ST_Intersection extends ScalarFunction {

        public Geometry eval(

                Object g1,

                Object g2) {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.intersection(geom1, geom2);
        }
    }

    public static class ST_Length extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.length(geom);
        }
    }

    public static class ST_Length2D extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.length(geom);
        }
    }

    public static class ST_LengthSpheroid extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.sphere.Spheroid.length(geom);
        }
    }

    public static class ST_LineInterpolatePoint extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double fraction) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.lineInterpolatePoint(geom, fraction);
        }
    }

    public static class ST_LineLocatePoint extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(
                byte[] ewkb,

                Object p) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            Geometry point = (Geometry) p;
            return org.apache.sedona.common.Functions.lineLocatePoint(geom, point);
        }
    }

    public static class ST_LocateAlong extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double measure,
                @DataTypeHint(value = "Double") Double offset) {
            Geometry linear = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.locateAlong(linear, measure, offset);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double measure) {
            Geometry linear = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.locateAlong(linear, measure);
        }
    }

    public static class ST_LongestLine extends ScalarFunction {

        public Geometry eval(

                Object g1,

                Object g2) {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.longestLine(geom1, geom2);
        }
    }

    public static class ST_YMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.yMin(geom);
        }
    }

    public static class ST_YMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.yMax(geom);
        }
    }

    public static class ST_ZMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.zMax(geom);
        }
    }

    public static class ST_ZMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.zMin(geom);
        }
    }

    public static class ST_NDims extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.nDims(geom);
        }
    }

    public static class ST_FlipCoordinates extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.flipCoordinates(geom);
        }
    }

    public static class ST_GeoHash extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                Object geometry,
                Integer precision) {
            Geometry geom = (Geometry) geometry;
            return org.apache.sedona.common.Functions.geohash(geom, precision);
        }
    }

    public static class ST_Perimeter extends ScalarFunction {
        @DataTypeHint(value = "Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.perimeter(geom);
        }

        @DataTypeHint(value = "Double")
        public Double eval(
                byte[] ewkb,
                Boolean use_spheroid) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.perimeter(geom, use_spheroid);
        }

        @DataTypeHint(value = "Double")
        public Double eval(
                byte[] ewkb,
                Boolean use_spheroid,
                boolean lenient) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.perimeter(geom, use_spheroid, lenient);
        }
    }

    public static class ST_Perimeter2D extends ScalarFunction {
        @DataTypeHint(value = "Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.perimeter(geom);
        }

        @DataTypeHint(value = "Double")
        public Double eval(
                byte[] ewkb,
                Boolean use_spheroid) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.perimeter(geom, use_spheroid);
        }

        @DataTypeHint(value = "Double")
        public Double eval(
                byte[] ewkb,
                Boolean use_spheroid,
                boolean lenient) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.perimeter(geom, use_spheroid, lenient);
        }
    }

    public static class ST_PointOnSurface extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.pointOnSurface(geom);
        }
    }

    public static class ST_ReducePrecision extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Integer") Integer precisionScale) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.reducePrecision(geom, precisionScale);
        }
    }

    public static class ST_Reverse extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.reverse(geom);
        }
    }

    public static class ST_GeometryN extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                int n) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometryN(geom, n);
        }
    }

    public static class ST_InteriorRingN extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                int n) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.interiorRingN(geom, n);
        }
    }

    public static class ST_PointN extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                int n) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.pointN(geom, n);
        }
    }

    public static class ST_NPoints extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.nPoints(geom);
        }
    }

    public static class ST_NumGeometries extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.numGeometries(geom);
        }
    }

    public static class ST_NumInteriorRings extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.numInteriorRings(geom);
        }
    }

    public static class ST_NumInteriorRing extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.numInteriorRings(geom);
        }
    }

    public static class ST_ExteriorRing extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.exteriorRing(geom);
        }
    }

    public static class ST_AsEWKT extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asEWKT(geom);
        }
    }

    public static class ST_AsText extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asWKT(geom);
        }
    }

    public static class ST_AsEWKB extends ScalarFunction {
        @DataTypeHint("Bytes")
        public byte[] eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asEWKB(geom);
        }
    }

    public static class ST_AsHEXEWKB extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(
                byte[] ewkb,
                @DataTypeHint("String") String endian) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asHexEWKB(geom, endian);
        }

        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asHexEWKB(geom);
        }
    }

    public static class ST_AsBinary extends ScalarFunction {
        @DataTypeHint("Bytes")
        public byte[] eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asEWKB(geom);
        }
    }

    public static class ST_AsGeoJSON extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asGeoJson(geom);
        }

        @DataTypeHint("String")
        public String eval(
                byte[] ewkb,
                String type) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asGeoJson(geom, type);
        }
    }

    public static class ST_AsGML extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asGML(geom);
        }
    }

    public static class ST_AsKML extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.asKML(geom);
        }
    }

    public static class ST_Force_2D extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force2D(geom);
        }
    }

    public static class ST_Force2D extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force2D(geom);
        }
    }

    public static class ST_IsEmpty extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isEmpty(geom);
        }
    }

    public static class ST_X extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.x(geom);
        }
    }

    public static class ST_Y extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.y(geom);
        }
    }

    public static class ST_Z extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.z(geom);
        }
    }

    public static class ST_Zmflag extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.zmFlag(geom);
        }
    }

    public static class ST_XMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.xMax(geom);
        }
    }

    public static class ST_XMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.xMin(geom);
        }
    }

    public static class ST_BuildArea extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.buildArea(geom);
        }
    }

    public static class ST_SetSRID extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                int srid) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.setSRID(geom, srid);
        }
    }

    public static class ST_SRID extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.getSRID(geom);
        }
    }

    public static class ST_IsClosed extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isClosed(geom);
        }
    }

    public static class ST_IsPolygonCW extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isPolygonCW(geom);
        }
    }

    public static class ST_IsRing extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isRing(geom);
        }
    }

    public static class ST_IsSimple extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isSimple(geom);
        }
    }

    public static class ST_IsValid extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isValid(geom);
        }

        @DataTypeHint("Boolean")
        public Boolean eval(
                byte[] ewkb,
                @DataTypeHint("Integer") Integer flag) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isValid(geom, flag);
        }
    }

    public static class ST_Normalize extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.normalize(geom);
        }
    }

    public static class ST_AddMeasure extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb1,
                @DataTypeHint(value = "Double") Double measureStart,
                @DataTypeHint(value = "Double") Double measureEnd) {
            Geometry geom = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.addMeasure(geom, measureStart, measureEnd);
        }
    }

    public static class ST_AddPoint extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry linestring = GeometrySerde.deserialize(ewkb1);
            Geometry point = GeometrySerde.deserialize(ewkb2);

            return org.apache.sedona.common.Functions.addPoint(linestring, point);
        }


        public Geometry eval(byte[] ewkb1, byte[] ewkb2, int position) {
            Geometry linestring = GeometrySerde.deserialize(ewkb1);
            Geometry point = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.addPoint(linestring, point, position);
        }
    }

    public static class ST_RemovePoint extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.removePoint(geom);
        }


        public Geometry eval(
                byte[] ewkb,
                int offset) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.removePoint(geom, offset);
        }
    }

    public static class ST_RemoveRepeatedPoints extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.removeRepeatedPoints(geom);
        }


        public Geometry eval(
                byte[] ewkb,
                double tolerance) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.removeRepeatedPoints(geom, tolerance);
        }
    }

    public static class ST_SetPoint extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, int position, byte[] ewkb2) {
            Geometry linestring = GeometrySerde.deserialize(ewkb1);
            Geometry point = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.setPoint(linestring, position, point);
        }
    }

    public static class ST_LineFromMultiPoint extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.lineFromMultiPoint(geom);
        }
    }

    public static class ST_LineSegments extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.lineSegments(geom);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(
                byte[] ewkb,
                @DataTypeHint(value = "Boolean") Boolean lenient) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.lineSegments(geom, lenient);
        }
    }

    public static class ST_LineMerge extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.lineMerge(geom);
        }
    }

    public static class ST_LineSubstring extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double startFraction,
                @DataTypeHint("Double") Double endFraction) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.lineSubString(geom, startFraction, endFraction);
        }
    }

    public static class ST_HasM extends ScalarFunction {
        @DataTypeHint("Boolean")
        public Boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.hasM(geom);
        }
    }

    public static class ST_HasZ extends ScalarFunction {
        @DataTypeHint("Boolean")
        public Boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.hasZ(geom);
        }
    }

    public static class ST_M extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.m(geom);
        }
    }

    public static class ST_MMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.mMin(geom);
        }
    }

    public static class ST_MMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.mMax(geom);
        }
    }

    public static class ST_MakeLine extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.makeLine(geom1, geom2);
        }


        public Geometry eval(@DataTypeHint(inputGroup = InputGroup.ANY) byte[][] ewkb) {
            Geometry[] geoms = Arrays.stream(ewkb).map(GeometrySerde::deserialize).toArray(Geometry[]::new);
            return org.apache.sedona.common.Functions.makeLine(geoms);
        }
    }

    public static class ST_MakePolygon extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, @DataTypeHint(inputGroup = InputGroup.ANY) byte[][] ewkb2) {
            Geometry outerLinestring = GeometrySerde.deserialize(ewkb1);
            Geometry[] interiorLinestrings = Arrays.stream(ewkb2).map(GeometrySerde::deserialize).toArray(Geometry[]::new);
            return org.apache.sedona.common.Functions.makePolygon(outerLinestring, interiorLinestrings);
        }


        public Geometry eval(byte[] ewkb) {
            Geometry linestring = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.makePolygon(linestring, null);
        }
    }

    public static class ST_Points extends ScalarFunction {

        public Geometry eval(byte[] ewkb1) {
            Geometry geom = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.points(geom);
        }
    }

    public static class ST_Polygon extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, @DataTypeHint("Integer") Integer srid) {
            Geometry linestring = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.makepolygonWithSRID(linestring, srid);
        }
    }

    public static class ST_Polygonize extends ScalarFunction {

        public Geometry eval(byte[] ewkb1) {
            Geometry geom = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.polygonize(geom);
        }
    }

    public static class ST_Project extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb1,
                @DataTypeHint(value = "Double") Double distance,
                @DataTypeHint(value = "Double") Double azimuth,
                @DataTypeHint("Boolean") Boolean lenient) {
            Geometry point = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.project(point, distance, azimuth, lenient);
        }


        public Geometry eval(

                byte[] ewkb1,
                @DataTypeHint(value = "Double") Double distance,
                @DataTypeHint(value = "Double") Double azimuth) {
            Geometry point = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.project(point, distance, azimuth);
        }
    }

    public static class ST_MakeValid extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Boolean") Boolean keepCollapsed) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.makeValid(geom, keepCollapsed);
        }


        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.makeValid(geom, false);
        }
    }

    public static class ST_MaxDistance extends ScalarFunction {
        @DataTypeHint(value = "Double")
        public Double eval(

                Object g1,

                Object g2) {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.maxDistance(geom1, geom2);
        }
    }

    public static class ST_MinimumClearance extends ScalarFunction {
        @DataTypeHint(value = "Double")
        public Double eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.minimumClearance(geometry);
        }
    }

    public static class ST_MinimumClearanceLine extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.minimumClearanceLine(geometry);
        }
    }

    public static class ST_MinimumBoundingCircle extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Integer") Integer quadrantSegments) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.minimumBoundingCircle(geom, quadrantSegments);
        }


        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.minimumBoundingCircle(
                    geom, BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6);
        }
    }

    public static class ST_MinimumBoundingRadius extends ScalarFunction {
        @DataTypeHint(value = "RAW")
        public Pair<Geometry, Double> eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.minimumBoundingRadius(geom);
        }
    }

    public static class ST_Multi extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.createMultiGeometryFromOneElement(geom);
        }
    }

    public static class ST_StartPoint extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.startPoint(geom);
        }
    }

    public static class ST_Split extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry input = GeometrySerde.deserialize(ewkb1);
            Geometry blade = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.split(input, blade);
        }
    }

    public static class ST_Snap extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2, @DataTypeHint("Double") Double tolerance) {
            Geometry input = GeometrySerde.deserialize(ewkb1);
            Geometry reference = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.snap(input, reference, tolerance);
        }
    }

    public static class ST_S2CellIDs extends ScalarFunction {
        @DataTypeHint(value = "ARRAY<BIGINT>")
        public Long[] eval(
                byte[] ewkb,
                @DataTypeHint("INT") Integer level) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.s2CellIDs(geom, level);
        }
    }

    public static class ST_S2ToGeom extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(@DataTypeHint(value = "ARRAY<BIGINT>") Long[] cellIds) {
            return org.apache.sedona.common.Functions.s2ToGeom(
                    Arrays.stream(cellIds).mapToLong(Long::longValue).toArray());
        }
    }

    public static class ST_H3CellIDs extends ScalarFunction {
        @DataTypeHint(value = "ARRAY<BIGINT>")
        public Long[] eval(
                byte[] ewkb,
                @DataTypeHint("INT") Integer level,
                @DataTypeHint("Boolean") Boolean fullCover) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.h3CellIDs(geom, level, fullCover);
        }
    }

    public static class ST_H3CellDistance extends ScalarFunction {
        @DataTypeHint(value = "BIGINT")
        public Long eval(@DataTypeHint("BIGINT") Long cell1, @DataTypeHint("BIGINT") Long cell2) {
            return org.apache.sedona.common.Functions.h3CellDistance(cell1, cell2);
        }
    }

    public static class ST_H3KRing extends ScalarFunction {
        @DataTypeHint(value = "ARRAY<BIGINT>")
        public Long[] eval(
                @DataTypeHint("BIGINT") Long cell,
                @DataTypeHint("INTEGER") Integer k,
                @DataTypeHint("Boolean") Boolean exactRing) {
            return org.apache.sedona.common.Functions.h3KRing(cell, k, exactRing);
        }
    }

    public static class ST_H3ToGeom extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(@DataTypeHint(value = "ARRAY<BIGINT>") Long[] cells) {
            return org.apache.sedona.common.Functions.h3ToGeom(
                    Arrays.stream(cells).mapToLong(Long::longValue).toArray());
        }
    }

    public static class ST_Simplify extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double distanceTolerance) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.simplify(geom, distanceTolerance);
        }
    }

    public static class ST_SimplifyPreserveTopology extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double distanceTolerance) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.simplifyPreserveTopology(geom, distanceTolerance);
        }
    }

    public static class ST_SimplifyVW extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double distanceTolerance) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.simplifyVW(geom, distanceTolerance);
        }
    }

    public static class ST_SimplifyPolygonHull extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double vertexFactor,
                @DataTypeHint("Boolean") Boolean isOuter) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.simplifyPolygonHull(geom, vertexFactor, isOuter);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double vertexFactor) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.simplifyPolygonHull(geom, vertexFactor);
        }
    }

    public static class ST_Subdivide extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry[].class)
        public Geometry[] eval(
                byte[] ewkb,
                @DataTypeHint("INT") Integer maxVertices) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.subDivide(geom, maxVertices);
        }
    }

    public static class ST_SymDifference extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.symDifference(geom1, geom2);
        }
    }

    public static class ST_GeometricMedian extends ScalarFunction {

        public Geometry eval(byte[] ewkb)
                throws Exception {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometricMedian(geometry);
        }


        public Geometry eval(byte[] ewkb, @DataTypeHint("Double") Double tolerance)
                throws Exception {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometricMedian(geometry, tolerance);
        }


        public Geometry eval(byte[] ewkb, @DataTypeHint("Double") Double tolerance, int maxIter)
                throws Exception {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometricMedian(geometry, tolerance, maxIter);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double tolerance,
                int maxIter,
                @DataTypeHint("Boolean") Boolean failIfNotConverged)
                throws Exception {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.geometricMedian(
                    geometry, tolerance, maxIter, failIfNotConverged);
        }
    }

    public static class ST_FrechetDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                Object g1,

                Object g2) {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.frechetDistance(geom1, geom2);
        }
    }

    public static class ST_NumPoints extends ScalarFunction {
        @DataTypeHint(value = "Integer")
        public int eval(
                byte[] ewkb)
                throws Exception {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.numPoints(geometry);
        }
    }

    public static class ST_Force3D extends ScalarFunction {


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double zValue) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force3D(geometry, zValue);
        }


        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force3D(geometry);
        }
    }

    public static class ST_Force3DM extends ScalarFunction {


        public Geometry eval(byte[] ewkb, @DataTypeHint("Double") Double zValue) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force3DM(geometry, zValue);
        }


        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force3DM(geometry);
        }
    }

    public static class ST_Force3DZ extends ScalarFunction {


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double zValue) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force3D(geometry, zValue);
        }


        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force3D(geometry);
        }
    }

    public static class ST_Force4D extends ScalarFunction {


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double zValue,
                @DataTypeHint("Double") Double mValue) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force4D(geometry, zValue, mValue);
        }


        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.force4D(geometry);
        }
    }

    public static class ST_ForceCollection extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.forceCollection(geometry);
        }
    }

    public static class ST_ForcePolygonCW extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.forcePolygonCW(geometry);
        }
    }

    public static class ST_ForceRHR extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.forcePolygonCW(geometry);
        }
    }

    public static class ST_GeneratePoints extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Integer") Integer numPoints) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.generatePoints(geom, numPoints);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Integer") Integer numPoints,
                @DataTypeHint(value = "BIGINT") Long seed) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.generatePoints(geom, numPoints, seed);
        }
    }

    public static class ST_NRings extends ScalarFunction {
        @DataTypeHint(value = "Integer")
        public int eval(
                byte[] ewkb)
                throws Exception {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.nRings(geom);
        }
    }

    public static class ST_ForcePolygonCCW extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.forcePolygonCCW(geometry);
        }
    }

    public static class ST_IsPolygonCCW extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isPolygonCCW(geom);
        }
    }

    public static class ST_Translate extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double deltaX,
                @DataTypeHint("Double") Double deltaY) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.translate(geometry, deltaX, deltaY);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double deltaX,
                @DataTypeHint("Double") Double deltaY,
                @DataTypeHint("Double") Double deltaZ) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.translate(geometry, deltaX, deltaY, deltaZ);
        }
    }

    public static class ST_TriangulatePolygon extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.triangulatePolygon(geometry);
        }
    }

    public static class ST_UnaryUnion extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.unaryUnion(geometry);
        }
    }

    public static class ST_Union extends ScalarFunction {

        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry a = GeometrySerde.deserialize(ewkb1);
            Geometry b = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.union(a, b);
        }


        public Geometry eval(@DataTypeHint(inputGroup = InputGroup.ANY) byte[][] ewkb) {
            Geometry[] geoms = Arrays.stream(ewkb).map(GeometrySerde::deserialize).toArray(Geometry[]::new);
            return org.apache.sedona.common.Functions.union(geoms);
        }
    }

    public static class ST_VoronoiPolygons extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double tolerance,

                Object extend) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            Geometry extendTo = (Geometry) extend;
            return FunctionsGeoTools.voronoiPolygons(geom, tolerance, extendTo);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double tolerance) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return FunctionsGeoTools.voronoiPolygons(geom, tolerance, null);
        }


        public Geometry eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return FunctionsGeoTools.voronoiPolygons(geom, 0, null);
        }
    }

    public static class ST_Affine extends ScalarFunction {

        public Geometry eval(
                @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) byte[] ewkb,
                @DataTypeHint("Double") Double a,
                @DataTypeHint("Double") Double b,
                @DataTypeHint("Double") Double c,
                @DataTypeHint("Double") Double d,
                @DataTypeHint("Double") Double e,
                @DataTypeHint("Double") Double f,
                @DataTypeHint("Double") Double g,
                @DataTypeHint("Double") Double h,
                @DataTypeHint("Double") Double i,
                @DataTypeHint("Double") Double xOff,
                @DataTypeHint("Double") Double yOff,
                @DataTypeHint("Double") Double zOff) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.affine(
                    geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint("Double") Double a,
                @DataTypeHint("Double") Double b,
                @DataTypeHint("Double") Double d,
                @DataTypeHint("Double") Double e,
                @DataTypeHint("Double") Double xOff,
                @DataTypeHint("Double") Double yOff) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.affine(geometry, a, b, d, e, xOff, yOff);
        }
    }

    public static class ST_BoundingDiagonal extends ScalarFunction {

        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.boundingDiagonal(geometry);
        }
    }

    public static class ST_HausdorffDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(

                Object g1,

                Object g2,
                @DataTypeHint("Double") Double densityFrac)
                throws Exception {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.hausdorffDistance(geom1, geom2, densityFrac);
        }

        @DataTypeHint("Double")
        public Double eval(

                Object g1,

                Object g2)
                throws Exception {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.hausdorffDistance(geom1, geom2);
        }
    }

    public static class ST_CoordDim extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.nDims(geom);
        }
    }

    public static class ST_IsCollection extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isCollection(geom);
        }
    }

    public static class ST_Angle extends ScalarFunction {

        @DataTypeHint("Double")
        public Double eval(

                Object p1,

                Object p2,

                Object p3,

                Object p4) {
            Geometry point1 = (Geometry) p1;
            Geometry point2 = (Geometry) p2;
            Geometry point3 = (Geometry) p3;
            Geometry point4 = (Geometry) p4;

            return org.apache.sedona.common.Functions.angle(point1, point2, point3, point4);
        }

        @DataTypeHint("Double")
        public Double eval(

                Object p1,

                Object p2,

                Object p3) {
            Geometry point1 = (Geometry) p1;
            Geometry point2 = (Geometry) p2;
            Geometry point3 = (Geometry) p3;

            return org.apache.sedona.common.Functions.angle(point1, point2, point3);
        }

        @DataTypeHint("Double")
        public Double eval(

                Object line1,

                Object line2) {
            Geometry lineString1 = (Geometry) line1;
            Geometry lineString2 = (Geometry) line2;

            return org.apache.sedona.common.Functions.angle(lineString1, lineString2);
        }
    }

    public static class ST_Degrees extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint("Double") Double angleInRadian) {
            return org.apache.sedona.common.Functions.degrees(angleInRadian);
        }
    }

    public static class ST_DelaunayTriangles extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double tolerance,
                @DataTypeHint(value = "Integer") Integer flag) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.delaunayTriangle(geometry, tolerance, flag);
        }


        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double tolerance) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.delaunayTriangle(geometry, tolerance);
        }


        public Geometry eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.delaunayTriangle(geometry);
        }
    }

    public static class ST_IsValidTrajectory extends ScalarFunction {
        @DataTypeHint("Boolean")
        public Boolean eval(byte[] ewkb) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isValidTrajectory(geometry);
        }
    }

    public static class ST_IsValidReason extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(

                byte[] ewkb) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isValidReason(geom);
        }

        @DataTypeHint("String")
        public String eval(
                byte[] ewkb,
                @DataTypeHint("Integer") Integer flag) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.isValidReason(geom, flag);
        }
    }

    public static class ST_Scale extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double scaleX,
                @DataTypeHint(value = "Double") Double scaleY) {
            Geometry geometry = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.scale(geometry, scaleX, scaleY);
        }
    }

    public static class ST_ScaleGeom extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb1,

                byte[] ewkb2,

                byte[] ewkb3) {
            Geometry geometry = GeometrySerde.deserialize(ewkb1);
            Geometry factor = GeometrySerde.deserialize(ewkb2);
            Geometry origin = GeometrySerde.deserialize(ewkb3);
            return org.apache.sedona.common.Functions.scaleGeom(geometry, factor, origin);
        }


        public Geometry eval(byte[] ewkb1, byte[] ewkb2) {
            Geometry geometry = GeometrySerde.deserialize(ewkb1);
            Geometry factor = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.scaleGeom(geometry, factor);
        }
    }

    public static class ST_RotateX extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double angle) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.rotateX(geom, angle);
        }
    }

    public static class ST_RotateY extends ScalarFunction {

        public Geometry eval(
                byte[] ewkb,
                @DataTypeHint(value = "Double") Double angle) {
            Geometry geom = GeometrySerde.deserialize(ewkb);
            return org.apache.sedona.common.Functions.rotateY(geom, angle);
        }
    }

    public static class ST_Rotate extends ScalarFunction {

        public Geometry eval(

                byte[] ewkb1,
                @DataTypeHint(value = "Double") Double angle) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.rotate(geom1, angle);
        }


        public Geometry eval(

                byte[] ewkb1,
                @DataTypeHint(value = "Double") Double angle,

                byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.rotate(geom1, angle, geom2);
        }


        public Geometry eval(

                byte[] ewkb1,
                @DataTypeHint(value = "Double") Double angle,
                @DataTypeHint(value = "Double") Double originX,
                @DataTypeHint(value = "Double") Double originY) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            return org.apache.sedona.common.Functions.rotate(geom1, angle, originX, originY);
        }
    }

    public static class ST_InterpolatePoint extends ScalarFunction {
        @DataTypeHint("Double")
        public double eval(
                @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) byte[] ewkb1,
                @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) byte[] ewkb2) {
            Geometry geom1 = GeometrySerde.deserialize(ewkb1);
            Geometry geom2 = GeometrySerde.deserialize(ewkb2);
            return org.apache.sedona.common.Functions.interpolatePoint(geom1, geom2);
        }
    }
}
