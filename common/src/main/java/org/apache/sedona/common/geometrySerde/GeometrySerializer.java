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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBConstants;

public class GeometrySerializer {
    private static final Coordinate NULL_COORDINATE = new Coordinate(Double.NaN, Double.NaN);
    private static final GeometryFactory FACTORY = new GeometryFactory();

    public static byte[] serialize(Geometry geometry) {
        GeometryBuffer buffer;
        if (geometry instanceof Point) {
            buffer = serializePoint((Point) geometry);
        } else if (geometry instanceof MultiPoint) {
            buffer = serializeMultiPoint((MultiPoint) geometry);
        } else if (geometry instanceof LineString) {
            buffer = serializeLineString((LineString) geometry);
        } else if (geometry instanceof MultiLineString) {
            buffer = serializeMultiLineString((MultiLineString) geometry);
        } else if (geometry instanceof Polygon) {
            buffer = serializePolygon((Polygon) geometry);
        } else if (geometry instanceof MultiPolygon) {
            buffer = serializeMultiPolygon((MultiPolygon) geometry);
        } else if (geometry instanceof GeometryCollection) {
            buffer = serializeGeometryCollection((GeometryCollection) geometry);
        } else {
            throw new UnsupportedOperationException(
                    "Geometry type is not supported: " + geometry.getClass().getSimpleName());
        }
        return buffer.toByteArray();
    }

    public static Geometry deserialize(byte[] bytes) {
        GeometryBuffer buffer = GeometryBufferFactory.wrap(bytes);
        return deserialize(buffer);
    }

    public static Geometry deserialize(GeometryBuffer buffer) {
        checkBufferSize(buffer, 8);
        int preambleByte = buffer.getByte(0) & 0xFF;
        int wkbType = preambleByte >> 4;
        CoordinateType coordType = CoordinateType.valueOf((preambleByte & 0x0F) >> 1);
        boolean hasSrid = (preambleByte & 0x01) != 0;
        buffer.setCoordinateType(coordType);
        int srid = 0;
        if (hasSrid) {
            int srid2 = (buffer.getByte(1) & 0xFF) << 16;
            int srid1 = (buffer.getByte(2) & 0xFF) << 8;
            int srid0 = buffer.getByte(3) & 0xFF;
            srid = (srid2 | srid1 | srid0);
        }
        return deserialize(buffer, wkbType, srid);
    }

    private static Geometry deserialize(GeometryBuffer buffer, int wkbType, int srid) {
        switch (wkbType) {
            case WKBConstants.wkbPoint:
                return deserializePoint(buffer, srid);
            case WKBConstants.wkbMultiPoint:
                return deserializeMultiPoint(buffer, srid);
            case WKBConstants.wkbLineString:
                return deserializeLineString(buffer, srid);
            case WKBConstants.wkbMultiLineString:
                return deserializeMultiLineString(buffer, srid);
            case WKBConstants.wkbPolygon:
                return deserializePolygon(buffer, srid);
            case WKBConstants.wkbMultiPolygon:
                return deserializeMultiPolygon(buffer, srid);
            case WKBConstants.wkbGeometryCollection:
                return deserializeGeometryCollection(buffer, srid);
            default:
                throw new IllegalArgumentException(
                        "Cannot deserialize buffer containing unknown geometry type ID: " + wkbType);
        }
    }

    private static GeometryBuffer serializePoint(Point point) {
        Coordinate coordinate = point.getCoordinate();
        if (coordinate == null) {
            return createGeometryBuffer(WKBConstants.wkbPoint, CoordinateType.XY, point.getSRID(), 8, 0);
        }
        CoordinateType coordType = getCoordinateType(coordinate);
        int bufferSize = 8 + coordType.bytes;
        GeometryBuffer buffer =
                createGeometryBuffer(WKBConstants.wkbPoint, coordType, point.getSRID(), bufferSize, 1);
        buffer.putCoordinate(8, coordinate);
        return buffer;
    }

    private static Point deserializePoint(GeometryBuffer buffer, int srid) {
        CoordinateType coordType = buffer.getCoordinateType();
        int numCoordinates = getBoundedInt(buffer, 4);
        Point point;
        if (numCoordinates == 0) {
            point = FACTORY.createPoint();
            buffer.mark(8);
        } else {
            int bufferSize = 8 + coordType.bytes;
            checkBufferSize(buffer, bufferSize);
            CoordinateSequence coordinates = buffer.getCoordinate(8);
            point = FACTORY.createPoint(coordinates);
            buffer.mark(bufferSize);
        }
        point.setSRID(srid);
        return point;
    }

    private static GeometryBuffer serializeMultiPoint(MultiPoint multiPoint) {
        int numPoints = multiPoint.getNumGeometries();
        if (numPoints == 0) {
            return createGeometryBuffer(
                    WKBConstants.wkbMultiPoint, CoordinateType.XY, multiPoint.getSRID(), 8, 0);
        }
        CoordinateType coordType = getCoordinateType(multiPoint);
        int bufferSize = 8 + numPoints * coordType.bytes;
        GeometryBuffer buffer =
                createGeometryBuffer(
                        WKBConstants.wkbMultiPoint, coordType, multiPoint.getSRID(), bufferSize, numPoints);
        for (int k = 0; k < numPoints; k++) {
            Point point = (Point) multiPoint.getGeometryN(k);
            Coordinate coordinate = point.getCoordinate();
            int coordinateOffset = 8 + k * coordType.bytes;
            if (coordinate == null) {
                buffer.putCoordinate(coordinateOffset, NULL_COORDINATE);
            } else {
                buffer.putCoordinate(coordinateOffset, coordinate);
            }
        }
        return buffer;
    }

    private static MultiPoint deserializeMultiPoint(GeometryBuffer buffer, int srid) {
        CoordinateType coordType = buffer.getCoordinateType();
        int numPoints = getBoundedInt(buffer, 4);
        int bufferSize = 8 + numPoints * coordType.bytes;
        checkBufferSize(buffer, bufferSize);
        Point[] points = new Point[numPoints];
        for (int i = 0; i < numPoints; i++) {
            CoordinateSequence coordinates = buffer.getCoordinate(8 + i * coordType.bytes);
            Coordinate coordinate = coordinates.getCoordinate(0);
            if (Double.isNaN(coordinate.x)) {
                points[i] = FACTORY.createPoint();
            } else {
                points[i] = FACTORY.createPoint(coordinates);
            }
            points[i].setSRID(srid);
        }
        buffer.mark(bufferSize);
        MultiPoint multiPoint = FACTORY.createMultiPoint(points);
        multiPoint.setSRID(srid);
        return multiPoint;
    }

    private static GeometryBuffer serializeLineString(LineString lineString) {
        CoordinateSequence coordinates = lineString.getCoordinateSequence();
        int numCoordinates = coordinates.size();
        if (numCoordinates == 0) {
            return createGeometryBuffer(
                    WKBConstants.wkbLineString, CoordinateType.XY, lineString.getSRID(), 8, 0);
        }
        CoordinateType coordType = getCoordinateType(coordinates.getCoordinate(0));
        int bufferSize = 8 + numCoordinates * coordType.bytes;
        GeometryBuffer buffer =
                createGeometryBuffer(
                        WKBConstants.wkbLineString,
                        coordType,
                        lineString.getSRID(),
                        bufferSize,
                        numCoordinates);
        buffer.putCoordinates(8, coordinates);
        return buffer;
    }

    private static LineString deserializeLineString(GeometryBuffer buffer, int srid) {
        CoordinateType coordType = buffer.getCoordinateType();
        int numCoordinates = getBoundedInt(buffer, 4);
        int bufferSize = 8 + numCoordinates * coordType.bytes;
        checkBufferSize(buffer, bufferSize);
        CoordinateSequence coordinates = buffer.getCoordinates(8, numCoordinates);
        buffer.mark(bufferSize);
        LineString lineString = FACTORY.createLineString(coordinates);
        lineString.setSRID(srid);
        return lineString;
    }

    private static GeometryBuffer serializeMultiLineString(MultiLineString multiLineString) {
        int numLineStrings = multiLineString.getNumGeometries();
        CoordinateType coordType = getCoordinateType(multiLineString);
        int numCoordinates = multiLineString.getNumPoints();
        int coordsOffset = 8;
        int numOffset = 8 + numCoordinates * coordType.bytes;
        int bufferSize = numOffset + 4 + numLineStrings * 4;
        GeometryBuffer buffer =
                createGeometryBuffer(
                        WKBConstants.wkbMultiLineString,
                        coordType,
                        multiLineString.getSRID(),
                        bufferSize,
                        numCoordinates);
        GeomPartSerializer serializer = new GeomPartSerializer(buffer, coordsOffset, numOffset);
        serializer.writeInt(numLineStrings);
        for (int k = 0; k < numLineStrings; k++) {
            LineString ls = (LineString) multiLineString.getGeometryN(k);
            serializer.write(ls);
        }
        assert bufferSize == serializer.intsOffset;
        return buffer;
    }

    private static MultiLineString deserializeMultiLineString(GeometryBuffer buffer, int srid) {
        CoordinateType coordType = buffer.getCoordinateType();
        int numCoordinates = getBoundedInt(buffer, 4);
        int coordsOffset = 8;
        int numOffset = 8 + numCoordinates * coordType.bytes;
        GeomPartSerializer serializer = new GeomPartSerializer(buffer, coordsOffset, numOffset);
        int numLineStrings = serializer.checkedReadBoundedInt();
        serializer.checkRemainingIntsAtLeast(numLineStrings);
        LineString[] lineStrings = new LineString[numLineStrings];
        for (int k = 0; k < numLineStrings; k++) {
            LineString ls = serializer.readLineString();
            ls.setSRID(srid);
            lineStrings[k] = ls;
        }
        serializer.markEndOfBuffer();
        MultiLineString multiLineString = FACTORY.createMultiLineString(lineStrings);
        multiLineString.setSRID(srid);
        return multiLineString;
    }

    private static GeometryBuffer serializePolygon(Polygon polygon) {
        LinearRing exteriorRing = polygon.getExteriorRing();
        if (exteriorRing == null || exteriorRing.isEmpty()) {
            return createGeometryBuffer(
                    WKBConstants.wkbPolygon, CoordinateType.XY, polygon.getSRID(), 8, 0);
        }
        CoordinateSequence coordinates = exteriorRing.getCoordinateSequence();
        CoordinateType coordType = getCoordinateType(coordinates.getCoordinate(0));
        int numCoordinates = polygon.getNumPoints();
        int numInteriorRings = polygon.getNumInteriorRing();
        int coordsOffset = 8;
        int numRingsOffset = 8 + numCoordinates * coordType.bytes;
        int bufferSize = numRingsOffset + 4 + 4 * (numInteriorRings + 1);
        GeometryBuffer buffer =
                createGeometryBuffer(
                        WKBConstants.wkbPolygon, coordType, polygon.getSRID(), bufferSize, numCoordinates);
        GeomPartSerializer serializer = new GeomPartSerializer(buffer, coordsOffset, numRingsOffset);
        serializer.write(polygon);
        assert bufferSize == serializer.intsOffset;
        return buffer;
    }

    private static Polygon deserializePolygon(GeometryBuffer buffer, int srid) {
        CoordinateType coordType = buffer.getCoordinateType();
        int numCoordinates = getBoundedInt(buffer, 4);
        if (numCoordinates == 0) {
            buffer.mark(8);
            Polygon polygon = FACTORY.createPolygon();
            polygon.setSRID(srid);
            return polygon;
        }
        int coordsOffset = 8;
        int numRingsOffset = 8 + numCoordinates * coordType.bytes;
        GeomPartSerializer serializer = new GeomPartSerializer(buffer, coordsOffset, numRingsOffset);
        Polygon polygon = serializer.readPolygon();
        serializer.markEndOfBuffer();
        polygon.setSRID(srid);
        return polygon;
    }

    private static GeometryBuffer serializeMultiPolygon(MultiPolygon multiPolygon) {
        int numPolygons = multiPolygon.getNumGeometries();
        int numCoordinates = 0;
        CoordinateType coordType = getCoordinateType(multiPolygon);
        int totalRings = 0;
        for (int k = 0; k < numPolygons; k++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(k);
            if (!polygon.isEmpty()) {
                int numRings = polygon.getNumInteriorRing() + 1;
                totalRings += numRings;
                numCoordinates += polygon.getNumPoints();
            }
        }
        int coordsOffset = 8;
        int numPolygonsOffset = 8 + numCoordinates * coordType.bytes;
        int bufferSize = numPolygonsOffset + 4 + (numPolygons * 4) + (totalRings * 4);
        GeometryBuffer buffer =
                createGeometryBuffer(
                        WKBConstants.wkbMultiPolygon,
                        coordType,
                        multiPolygon.getSRID(),
                        bufferSize,
                        numCoordinates);
        GeomPartSerializer serializer = new GeomPartSerializer(buffer, coordsOffset, numPolygonsOffset);
        serializer.writeInt(numPolygons);
        for (int k = 0; k < numPolygons; k++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(k);
            serializer.write(polygon);
        }
        assert bufferSize == serializer.intsOffset;
        return buffer;
    }

    private static MultiPolygon deserializeMultiPolygon(GeometryBuffer buffer, int srid) {
        CoordinateType coordType = buffer.getCoordinateType();
        int numCoordinates = getBoundedInt(buffer, 4);
        int coordsOffset = 8;
        int numPolygonsOffset = 8 + numCoordinates * coordType.bytes;
        GeomPartSerializer serializer = new GeomPartSerializer(buffer, coordsOffset, numPolygonsOffset);
        int numPolygons = serializer.checkedReadBoundedInt();
        Polygon[] polygons = new Polygon[numPolygons];
        for (int k = 0; k < numPolygons; k++) {
            Polygon polygon = serializer.readPolygon();
            polygon.setSRID(srid);
            polygons[k] = polygon;
        }
        serializer.markEndOfBuffer();
        MultiPolygon multiPolygon = FACTORY.createMultiPolygon(polygons);
        multiPolygon.setSRID(srid);
        return multiPolygon;
    }

    private static GeometryBuffer serializeGeometryCollection(GeometryCollection geometryCollection) {
        int numGeometries = geometryCollection.getNumGeometries();
        if (numGeometries == 0) {
            return createGeometryBuffer(
                    WKBConstants.wkbGeometryCollection,
                    CoordinateType.XY,
                    geometryCollection.getSRID(),
                    8,
                    0);
        }
        byte[][] buffers = new byte[numGeometries][];
        int totalBytes = 0;
        for (int k = 0; k < numGeometries; k++) {
            byte[] buf = serialize(geometryCollection.getGeometryN(k));
            buffers[k] = buf;
            totalBytes += alignedOffset(buf.length);
        }
        int bufferSize = 8 + totalBytes;
        GeometryBuffer buffer =
                createGeometryBuffer(
                        WKBConstants.wkbGeometryCollection,
                        CoordinateType.XY,
                        geometryCollection.getSRID(),
                        bufferSize,
                        numGeometries);
        int offset = 8;
        for (int k = 0; k < numGeometries; k++) {
            byte[] buf = buffers[k];
            buffer.putBytes(offset, buf);
            offset += alignedOffset(buf.length);
        }
        assert offset == bufferSize;
        return buffer;
    }

    private static GeometryCollection deserializeGeometryCollection(GeometryBuffer buffer, int srid) {
        int numGeometries = getBoundedInt(buffer, 4);
        if (numGeometries == 0) {
            buffer.mark(8);
            GeometryCollection geometryCollection = FACTORY.createGeometryCollection();
            geometryCollection.setSRID(srid);
            return geometryCollection;
        }
        Geometry[] geometries = new Geometry[numGeometries];
        int offset = 8;
        for (int k = 0; k < numGeometries; k++) {
            GeometryBuffer geomBuffer = buffer.slice(offset);
            Geometry geometry = deserialize(geomBuffer);
            int geomLength = alignedOffset(geomBuffer.getMark());
            geometry.setSRID(srid);
            geometries[k] = geometry;
            offset += geomLength;
        }
        buffer.mark(offset);
        GeometryCollection geometryCollection = FACTORY.createGeometryCollection(geometries);
        geometryCollection.setSRID(srid);
        return geometryCollection;
    }

    private static GeometryBuffer createGeometryBuffer(
            int wkbType, CoordinateType coordType, int srid, int bufferSize, int numCoordinates) {
        GeometryBuffer buffer = GeometryBufferFactory.create(bufferSize);
        buffer.setCoordinateType(coordType);

        // Set header bytes [preamble][srid (3 bytes)][numCoordinates (4 bytes)]
        int hasSridBit = (srid != 0 ? 1 : 0);
        int preambleByte = (wkbType << 4) | (coordType.value << 1) | hasSridBit;
        buffer.putByte(0, (byte) preambleByte);
        if (srid != 0) {
            // Store SRID in the next 3 bytes in big endian byte order, with the highest bit set
            // indicating that SRID need to be decoded when deserializing it.
            buffer.putByte(1, (byte) (srid >> 16));
            buffer.putByte(2, (byte) (srid >> 8));
            buffer.putByte(3, (byte) srid);
        }
        buffer.putInt(4, numCoordinates);
        return buffer;
    }

    private static void checkBufferSize(GeometryBuffer buffer, int minimumSize) {
        if (buffer.getLength() < minimumSize) {
            throw new IllegalArgumentException("Buffer to be deserialized is incomplete");
        }
    }

    private static int getBoundedInt(GeometryBuffer buffer, int offset) {
        int value = buffer.getInt(offset);
        if (value < 0) {
            throw new IllegalArgumentException("Unexpected negative value encountered: " + value);
        }
        if (value > buffer.getLength()) {
            throw new IllegalArgumentException("Unexpected large value encountered: " + value);
        }
        return value;
    }

    private static CoordinateType getCoordinateType(Coordinate coordinate) {
        boolean hasZ = !Double.isNaN(coordinate.getZ());
        boolean hasM = !Double.isNaN(coordinate.getM());
        return getCoordinateType(hasZ, hasM);
    }

    private static CoordinateType getCoordinateType(Geometry geometry) {
        Coordinate coord = geometry.getCoordinate();
        if (coord != null) {
            return getCoordinateType(coord);
        } else {
            return CoordinateType.XY;
        }
    }

    private static CoordinateType getCoordinateType(boolean hasZ, boolean hasM) {
        if (hasZ && hasM) {
            return CoordinateType.XYZM;
        } else if (hasZ) {
            return CoordinateType.XYZ;
        } else if (hasM) {
            return CoordinateType.XYM;
        } else {
            return CoordinateType.XY;
        }
    }

    private static int alignedOffset(int offset) {
        return (offset + 7) & ~7;
    }

    static class GeomPartSerializer {
        final GeometryBuffer buffer;
        int coordsOffset;
        final int coordsEndOffset;
        int intsOffset;

        GeomPartSerializer(GeometryBuffer buffer, int coordsOffset, int intsOffset) {
            this.buffer = buffer;
            this.coordsOffset = coordsOffset;
            this.coordsEndOffset = intsOffset;
            this.intsOffset = intsOffset;
        }

        LineString readLineString() {
            CoordinateSequence coordinates = readCoordinates();
            return FACTORY.createLineString(coordinates);
        }

        LinearRing readRing() {
            CoordinateSequence coordinates = readCoordinates();
            return FACTORY.createLinearRing(coordinates);
        }

        Polygon readPolygon() {
            int numRings = checkedReadBoundedInt();
            if (numRings == 0) {
                return FACTORY.createPolygon();
            }
            checkRemainingIntsAtLeast(numRings);
            int numInteriorRings = numRings - 1;
            LinearRing shell = readRing();
            LinearRing[] holes = new LinearRing[numInteriorRings];
            for (int k = 0; k < numInteriorRings; k++) {
                holes[k] = readRing();
            }
            return FACTORY.createPolygon(shell, holes);
        }

        CoordinateSequence readCoordinates() {
            int numCoordinates = getBoundedInt(buffer, intsOffset);
            int newCoordsOffset = coordsOffset + buffer.getCoordinateType().bytes * numCoordinates;
            if (newCoordsOffset > coordsEndOffset) {
                throw new IllegalStateException(
                        "Number of coordinates exceeds the capacity of buffer: " + numCoordinates);
            }
            CoordinateSequence coordinates = buffer.getCoordinates(coordsOffset, numCoordinates);
            coordsOffset = newCoordsOffset;
            intsOffset += 4;
            return coordinates;
        }

        int readBoundedInt() {
            int value = getBoundedInt(buffer, intsOffset);
            intsOffset += 4;
            return value;
        }

        int checkedReadBoundedInt() {
            checkBufferSize(buffer, intsOffset + 4);
            return readBoundedInt();
        }

        void checkRemainingIntsAtLeast(int num) {
            checkBufferSize(buffer, intsOffset + 4 * num);
        }

        void write(LineString lineString) {
            CoordinateSequence coordinates = lineString.getCoordinateSequence();
            int numCoordinates = coordinates.size();
            buffer.putCoordinates(coordsOffset, coordinates);
            buffer.putInt(intsOffset, numCoordinates);
            coordsOffset += numCoordinates * buffer.getCoordinateType().bytes;
            intsOffset += 4;
        }

        void write(Polygon polygon) {
            LinearRing exteriorRing = polygon.getExteriorRing();
            if (exteriorRing.isEmpty()) {
                writeInt(0);
                return;
            }
            int numInteriorRings = polygon.getNumInteriorRing();
            writeInt(numInteriorRings + 1);
            write(exteriorRing);
            for (int k = 0; k < numInteriorRings; k++) {
                write(polygon.getInteriorRingN(k));
            }
        }

        void writeInt(int value) {
            buffer.putInt(intsOffset, value);
            intsOffset += 4;
        }

        void markEndOfBuffer() {
            buffer.mark(intsOffset);
        }
    }
}
