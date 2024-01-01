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
package org.apache.sedona.common;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import static org.junit.Assert.*;

public class PredicatesTest extends TestBase {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    public void testDWithinSuccess() {
        Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2));
        double distance = 1.42;
        boolean actual = Predicates.dWithin(point1, point2, distance);
        assertTrue(actual);
    }

    @Test
    public void testDWithinFailure() {
        Geometry polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 1, 1, 1, 1, 0, 0, 0));
        Geometry polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 0, 3, 3, 6, 3, 6, 0, 3, 0));


        double distance = 1.2;
        boolean actual = Predicates.dWithin(polygon1, polygon2, distance);
        assertFalse(actual);
    }

    @Test
    public void testDWithinGeomCollection() {
        Geometry polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 1, 1, 1, 1, 0, 0, 0));
        Geometry polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 0, 3, 3, 6, 3, 6, 0, 3, 0));
        Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(1.1, 0));
        Geometry geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {polygon2, point});


        double distance = 1.2;
        boolean actual = Predicates.dWithin(polygon1, geometryCollection, distance);
        assertTrue(actual);
    }

    @Test
    public void testDWithinException() {
        Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2));
        Geometry transformedPoint2 = Functions.setSRID(point2, 4326);
        double distance = 1.42;
        Exception e = assertThrows(IllegalArgumentException.class, () -> Predicates.dWithin(point1, transformedPoint2, distance));
        String expectedMessage = "Provided left and right geometries do not share the same SRID. SRIDs are 0 and 4326 respectively.";
        assertEquals(expectedMessage, e.getMessage());
    }


}
