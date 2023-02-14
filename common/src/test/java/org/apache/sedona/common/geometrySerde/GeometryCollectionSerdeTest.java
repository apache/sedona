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

package org.apache.sedona.common.geometrySerde;

import org.junit.Test;
import org.junit.Assert;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import javax.sound.sampled.Line;

public class GeometryCollectionSerdeTest {
    private static final GeometryFactory gf = new GeometryFactory();

    @Test
    public void testEmptyGeometryCollection() {
        GeometryCollection geometryCollection = gf.createGeometryCollection();
        geometryCollection.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(geometryCollection);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof GeometryCollection);
        Assert.assertTrue(geom.isEmpty());
        Assert.assertEquals(4326, geom.getSRID());
    }

    @Test
    public void testGeometryCollection() {
        Point point = gf.createPoint(new Coordinate(10, 20));
        LineString lineString =
                gf.createLineString(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 1)});
        Polygon polygon =
                gf.createPolygon(
                        gf.createLinearRing(
                                new Coordinate[]{
                                        new Coordinate(0, 0),
                                        new Coordinate(0, 1),
                                        new Coordinate(1, 1),
                                        new Coordinate(1, 0),
                                        new Coordinate(0, 0)
                                }),
                        null);
        MultiPoint multiPoint =
                gf.createMultiPointFromCoords(
                        new Coordinate[]{
                                new Coordinate(10, 20), new Coordinate(30, 40), new Coordinate(50, 60)
                        });
        MultiLineString multiLineString =
                gf.createMultiLineString(new LineString[]{lineString, lineString, lineString});
        MultiPolygon multiPolygon = gf.createMultiPolygon(new Polygon[]{polygon, polygon});
        GeometryCollection geometryCollection =
                gf.createGeometryCollection(
                        new Geometry[]{
                                gf.createPoint(),
                                gf.createLineString(),
                                gf.createPolygon(),
                                point,
                                lineString,
                                polygon,
                                gf.createMultiPoint(),
                                gf.createMultiLineString(),
                                gf.createMultiPolygon(),
                                gf.createMultiPoint(new Point[] {
                                        gf.createPoint(),
                                        gf.createPoint()
                                }),
                                gf.createMultiLineString(new LineString[] {
                                        gf.createLineString(),
                                        gf.createLineString()
                                }),
                                gf.createMultiPolygon(new Polygon[] {
                                        gf.createPolygon(),
                                        gf.createPolygon(),
                                        gf.createPolygon()
                                }),
                                multiPoint,
                                multiLineString,
                                multiPolygon,
                                point
                        });
        geometryCollection.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(geometryCollection);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof GeometryCollection);
        Assert.assertEquals(4326, geom.getSRID());
        Assert.assertEquals(geometryCollection.getNumGeometries(), geom.getNumGeometries());
        for (int k = 0; k < geom.getNumGeometries(); k++) {
            Assert.assertEquals(geometryCollection.getGeometryN(k), geom.getGeometryN(k));
        }
    }

    @Test
    public void testNestedGeometryCollection() {
        Point point = gf.createPoint(new Coordinate(10, 20));
        LineString lineString =
                gf.createLineString(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 1)});
        MultiLineString multiLineString =
                gf.createMultiLineString(new LineString[]{lineString, lineString, lineString});
        GeometryCollection geomCollection1 =
                gf.createGeometryCollection(new Geometry[]{point, lineString, multiLineString});
        GeometryCollection geomCollection2 =
                gf.createGeometryCollection(
                        new Geometry[]{
                                point, geomCollection1, geomCollection1, multiLineString, geomCollection1
                        });
        geomCollection2.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(geomCollection2);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof GeometryCollection);
        Assert.assertEquals(4326, geom.getSRID());
        Assert.assertEquals(geomCollection2.getNumGeometries(), geom.getNumGeometries());
        for (int k = 0; k < geom.getNumGeometries(); k++) {
            Assert.assertEquals(geomCollection2.getGeometryN(k), geom.getGeometryN(k));
        }
    }
}
