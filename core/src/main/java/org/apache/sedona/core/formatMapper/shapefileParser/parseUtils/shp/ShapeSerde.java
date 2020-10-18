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

package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

import com.esotericsoftware.kryo.io.Input;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Provides methods to efficiently serialize and deserialize geometry types
 * using shapefile format developed by ESRI. Does not serialize user data
 * attached to the geometry.
 * <p>
 * Supports Point, LineString, Polygon, MultiPoint, MultiLineString and
 * MultiPolygon types.
 * <p>
 * Compatible with the family of {@link ShapeReader} classes.
 * <p>
 * First byte contains {@link ShapeType#id}. The rest is type specific.
 * Point: 8 bytes for X coordinate, followed by 8 bytes for Y coordinate.
 * LineString is serialized as MultiLineString.
 * MultiLineString: 16 bytes for envelope, 4 bytes for the number of line strings,
 * 4 bytes for total number of vertexes, 16 * num-vertexes for
 * XY coordinates of all the vertexes.
 * Polygons is serialized as MultiPolygon.
 * MultiPolygon: 16 bytes for envelope, 4 bytes for the total number of exterior and
 * interior rings of all polygons, 4 bytes for total number of vertexes,
 * 16 * num-vertexes for XY coordinates of all the vertexes. The vertexes
 * are written one polygon at a time, exterior ring first, followed by
 * interior rings.
 */
public class ShapeSerde
{
    private static final int POINT_LENGTH = 1 + 2 * ShapeFileConst.DOUBLE_LENGTH;

    public static byte[] serialize(Geometry geometry)
    {
        if (geometry instanceof Point) {
            return serialize((Point) geometry);
        }

        if (geometry instanceof MultiPoint) {
            return serialize((MultiPoint) geometry);
        }

        if (geometry instanceof LineString) {
            return serialize((LineString) geometry);
        }

        if (geometry instanceof MultiLineString) {
            return serialize((MultiLineString) geometry);
        }

        if (geometry instanceof Polygon) {
            return serialize((Polygon) geometry);
        }

        if (geometry instanceof MultiPolygon) {
            return serialize((MultiPolygon) geometry);
        }

        throw new UnsupportedOperationException("Geometry type is not supported: " +
                geometry.getClass().getSimpleName());
    }

    public static Geometry deserialize(Input input, GeometryFactory factory)
    {
        ShapeReader reader = ShapeReaderFactory.fromInput(input);
        ShapeType type = ShapeType.getType(reader.readByte());
        ShapeParser parser = type.getParser(factory);
        return parser.parseShape(reader);
    }

    public static Geometry deserialize(byte[] input, GeometryFactory factory)
    {
        ShapeReader reader = ShapeReaderFactory.fromByteBuffer(ByteBuffer.wrap(input));
        ShapeType type = ShapeType.getType(reader.readByte());
        ShapeParser parser = type.getParser(factory);
        return parser.parseShape(reader);
    }

    private static byte[] serialize(Point point)
    {
        ByteBuffer buffer = newBuffer(POINT_LENGTH);
        putType(buffer, ShapeType.POINT);
        buffer.putDouble(point.getX());
        buffer.putDouble(point.getY());

        return buffer.array();
    }

    private static void putType(ByteBuffer buffer, ShapeType type)
    {
        buffer.put((byte) type.getId());
    }

    private static byte[] serialize(MultiPoint multiPoint)
    {
        int numPoints = multiPoint.getNumPoints();

        ByteBuffer buffer = newBuffer(calculateBufferSize(multiPoint));
        putType(buffer, ShapeType.MULTIPOINT);
        buffer.position(buffer.position() + 4 * ShapeFileConst.DOUBLE_LENGTH);
        buffer.putInt(numPoints);
        for (int i = 0; i < numPoints; i++) {
            Point point = (Point) multiPoint.getGeometryN(i);
            buffer.putDouble(point.getX());
            buffer.putDouble(point.getY());
        }
        return buffer.array();
    }

    private static int calculateBufferSize(MultiPoint multiPoint)
    {
        return 1 + 4 * ShapeFileConst.DOUBLE_LENGTH + ShapeFileConst.INT_LENGTH + multiPoint.getNumPoints() * 2 * ShapeFileConst.DOUBLE_LENGTH;
    }

    private static byte[] serialize(LineString lineString)
    {
        int numPoints = lineString.getNumPoints();

        ByteBuffer buffer = newBuffer(calculateBufferSize(numPoints, 1));
        putHeader(buffer, ShapeType.POLYLINE, numPoints, 1);
        buffer.putInt(0);
        putPoints(buffer, lineString);
        return buffer.array();
    }

    private static int calculateBufferSize(int numPoints, int numParts)
    {
        return 1 + 4 * ShapeFileConst.DOUBLE_LENGTH + ShapeFileConst.INT_LENGTH + ShapeFileConst.INT_LENGTH + numParts * ShapeFileConst.INT_LENGTH + numPoints * 2 * ShapeFileConst.DOUBLE_LENGTH;
    }

    private static void putHeader(ByteBuffer buffer, ShapeType type, int numPoints, int numParts)
    {
        putType(buffer, type);
        buffer.position(buffer.position() + 4 * ShapeFileConst.DOUBLE_LENGTH);
        buffer.putInt(numParts);
        buffer.putInt(numPoints);
    }

    private static byte[] serialize(MultiLineString multiLineString)
    {
        int numPoints = multiLineString.getNumPoints();
        int numParts = multiLineString.getNumGeometries();

        ByteBuffer buffer = newBuffer(calculateBufferSize(numPoints, numParts));
        putHeader(buffer, ShapeType.POLYLINE, numPoints, numParts);

        int offset = 0;
        for (int i = 0; i < numParts; i++) {
            buffer.putInt(offset);
            offset += multiLineString.getGeometryN(i).getNumPoints();
        }

        for (int i = 0; i < numParts; i++) {
            putPoints(buffer, (LineString) multiLineString.getGeometryN(i));
        }
        return buffer.array();
    }

    private static byte[] serialize(Polygon polygon)
    {
        int numRings = polygon.getNumInteriorRing() + 1;
        int numPoints = polygon.getNumPoints();

        ByteBuffer buffer = newBuffer(calculateBufferSize(numPoints, numRings));
        putHeader(buffer, ShapeType.POLYGON, numPoints, numRings);
        putRingOffsets(buffer, polygon, 0);
        putPolygonPoints(buffer, polygon);
        return buffer.array();
    }

    private static int putRingOffsets(ByteBuffer buffer, Polygon polygon, int initialOffset)
    {
        int offset = initialOffset;
        int numRings = polygon.getNumInteriorRing() + 1;

        buffer.putInt(offset);
        offset += polygon.getExteriorRing().getNumPoints();
        for (int i = 0; i < numRings - 1; i++) {
            buffer.putInt(offset);
            offset += polygon.getInteriorRingN(i).getNumPoints();
        }

        return offset;
    }

    private static byte[] serialize(MultiPolygon multiPolygon)
    {
        int numPolygons = multiPolygon.getNumGeometries();
        int numPoints = multiPolygon.getNumPoints();

        int numRings = 0;
        for (int i = 0; i < numPolygons; i++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
            numRings += polygon.getNumInteriorRing() + 1;
        }

        ByteBuffer buffer = newBuffer(calculateBufferSize(numPoints, numRings));
        putHeader(buffer, ShapeType.POLYGON, numPoints, numRings);

        int offset = 0;
        for (int i = 0; i < numPolygons; i++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
            offset = putRingOffsets(buffer, polygon, offset);
        }

        for (int i = 0; i < numPolygons; i++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
            putPolygonPoints(buffer, polygon);
        }
        return buffer.array();
    }

    private static void putPolygonPoints(ByteBuffer buffer, Polygon polygon)
    {
        putPoints(buffer, polygon.getExteriorRing());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            putPoints(buffer, polygon.getInteriorRingN(i));
        }
    }

    private static void putPoints(ByteBuffer buffer, LineString geometry)
    {
        int numPoints = geometry.getNumPoints();
        for (int i = 0; i < numPoints; i++) {
            Point point = geometry.getPointN(i);
            buffer.putDouble(point.getX());
            buffer.putDouble(point.getY());
        }
    }

    private static ByteBuffer newBuffer(int size)
    {
        return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }
}
