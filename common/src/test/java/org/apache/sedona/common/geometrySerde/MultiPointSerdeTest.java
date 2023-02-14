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
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;

public class MultiPointSerdeTest {
    private static final GeometryFactory gf = new GeometryFactory();

    @Test
    public void testEmptyMultiPoint() {
        MultiPoint multiPoint = gf.createMultiPoint();
        multiPoint.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(multiPoint);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof MultiPoint);
        Assert.assertTrue(geom.isEmpty());
        Assert.assertEquals(4326, geom.getSRID());
    }

    @Test
    public void testMultiPoint() {
        MultiPoint multiPoint =
                gf.createMultiPointFromCoords(
                        new Coordinate[]{
                                new Coordinate(1, 2), new Coordinate(3, 4), new Coordinate(5, 6),
                        });
        multiPoint.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(multiPoint);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof MultiPoint);
        Assert.assertEquals(4326, geom.getSRID());
        MultiPoint multiPoint2 = (MultiPoint) geom;
        Assert.assertEquals(3, multiPoint2.getNumGeometries());
        Assert.assertEquals(1, multiPoint2.getGeometryN(0).getCoordinate().x, 1e-6);
        Assert.assertEquals(2, multiPoint2.getGeometryN(0).getCoordinate().y, 1e-6);
        Assert.assertEquals(3, multiPoint2.getGeometryN(1).getCoordinate().x, 1e-6);
        Assert.assertEquals(4, multiPoint2.getGeometryN(1).getCoordinate().y, 1e-6);
        Assert.assertEquals(5, multiPoint2.getGeometryN(2).getCoordinate().x, 1e-6);
        Assert.assertEquals(6, multiPoint2.getGeometryN(2).getCoordinate().y, 1e-6);
    }

    @Test
    public void testMultiPointWithEmptyPoints() {
        Point[] points =
                new Point[]{
                        gf.createPoint(new Coordinate(1, 2)),
                        gf.createPoint(),
                        gf.createPoint(new Coordinate(3, 4))
                };
        MultiPoint multiPoint = gf.createMultiPoint(points);
        byte[] bytes = GeometrySerializer.serialize(multiPoint);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof MultiPoint);
        Assert.assertEquals(0, geom.getSRID());
        MultiPoint multiPoint2 = (MultiPoint) geom;
        Assert.assertEquals(3, multiPoint2.getNumGeometries());
        Assert.assertEquals(1, multiPoint2.getGeometryN(0).getCoordinate().x, 1e-6);
        Assert.assertEquals(2, multiPoint2.getGeometryN(0).getCoordinate().y, 1e-6);
        Assert.assertTrue(multiPoint2.getGeometryN(1).isEmpty());
        Assert.assertEquals(3, multiPoint2.getGeometryN(2).getCoordinate().x, 1e-6);
        Assert.assertEquals(4, multiPoint2.getGeometryN(2).getCoordinate().y, 1e-6);
    }

    @Test
    public void testMultiPointWithEmptyPointsOnly() {
        Point[] points =
                new Point[]{
                        gf.createPoint(),
                        gf.createPoint(),
                        gf.createPoint()
                };
        MultiPoint multiPoint = gf.createMultiPoint(points);
        byte[] bytes = GeometrySerializer.serialize(multiPoint);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertEquals(3, geom.getNumGeometries());
        Assert.assertEquals(multiPoint, geom);
    }

    @Test
    public void testMultiPointXYM() {
        MultiPoint multiPoint =
                gf.createMultiPointFromCoords(
                        new Coordinate[]{
                                new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6), new CoordinateXYM(7, 8, 9),
                        });
        multiPoint.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(multiPoint);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof MultiPoint);
        Assert.assertEquals(4326, geom.getSRID());
        MultiPoint multiPoint2 = (MultiPoint) geom;
        Assert.assertEquals(3, multiPoint2.getNumGeometries());
        Assert.assertEquals(1, multiPoint2.getGeometryN(0).getCoordinate().x, 1e-6);
        Assert.assertEquals(2, multiPoint2.getGeometryN(0).getCoordinate().y, 1e-6);
        Assert.assertEquals(3, multiPoint2.getGeometryN(0).getCoordinate().getM(), 1e-6);
        Assert.assertEquals(4, multiPoint2.getGeometryN(1).getCoordinate().x, 1e-6);
        Assert.assertEquals(5, multiPoint2.getGeometryN(1).getCoordinate().y, 1e-6);
        Assert.assertEquals(6, multiPoint2.getGeometryN(1).getCoordinate().getM(), 1e-6);
        Assert.assertEquals(7, multiPoint2.getGeometryN(2).getCoordinate().x, 1e-6);
        Assert.assertEquals(8, multiPoint2.getGeometryN(2).getCoordinate().y, 1e-6);
        Assert.assertEquals(9, multiPoint2.getGeometryN(2).getCoordinate().getM(), 1e-6);
    }
}
