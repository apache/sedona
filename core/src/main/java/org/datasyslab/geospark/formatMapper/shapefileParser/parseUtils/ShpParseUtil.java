package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.*;
import io.netty.buffer.ByteBuf;
import org.apache.commons.io.EndianUtils;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zongsizhang on 5/3/17.
 */
public class ShpParseUtil implements ShapeFileConst{
    public static int currentTokenType = 0;
    public static long fileLength = 0;
    public static long remainLength = 0;

    public static GeometryFactory geometryFactory = null;

    public static void parseShapeFileHead(DataInputStream inputStream)
    throws IOException
    {
        int fileCode = inputStream.readInt();
        inputStream.skip(HEAD_EMPTY_NUM * INT_LENGTH);
        fileLength = 16 * inputStream.readInt() - HEAD_FILE_LENGTH_16BIT * 16;
        remainLength = fileLength;
        int fileVersion = EndianUtils.swapInteger(inputStream.readInt());
        currentTokenType = EndianUtils.swapInteger(inputStream.readInt());
        inputStream.skip(HEAD_BOX_NUM * DOUBLE_LENGTH);
    }

    public static void initializeGeometryFactory(){
        geometryFactory = new GeometryFactory();
    }

    public static byte[] parseRecordPrimitiveContent(DataInputStream inputStream) throws IOException{
        int contentLength = inputStream.readInt();
        long recordLength = 16 * (contentLength + 4);
        remainLength -= recordLength;
        byte[] contentArray = new byte[contentLength * 2];
        inputStream.readFully(contentArray,0,contentArray.length);
        return contentArray;
    }

    public static CoordinateSequence readCoordinates(ByteBuffer byteBuffer, int numPoints){
        CoordinateSequence coordinateSequence = geometryFactory.getCoordinateSequenceFactory().create(numPoints,2);
        DoubleBuffer dbuffer = byteBuffer.asDoubleBuffer();
        double[] ordinates = new double[numPoints*2];
        dbuffer.get(ordinates);
        for(int i = 0;i < numPoints; ++i){
            coordinateSequence.setOrdinate(i, 0, ordinates[i*2]);
            coordinateSequence.setOrdinate(i, 1, ordinates[i*2 + 1]);
        }
        return coordinateSequence;
    }

    public static int parseRecordHeadID(DataInputStream inputStream) throws IOException{
        int id = inputStream.readInt();
        return id;
    }

    public static void validateShapeType(ByteBuffer byteBuffer)
            throws IOException, ShapeTypeNotMatchException
    {
        int recordTokenType = byteBuffer.getInt();
        if(recordTokenType != currentTokenType) throw new ShapeTypeNotMatchException();
    }

    public static Point parsePoint(ByteBuffer byteBuffer)
            throws IOException
    {
        double x = byteBuffer.getDouble();
        double y = byteBuffer.getDouble();
        Point point = geometryFactory.createPoint(new Coordinate(x, y));
        return point;
    }

    /**
     * This is for parsing objects with shape type = MultiPoint
     * @param byteBuffer
     * @return
     * @throws IOException
     */
    public static MultiPoint parseMultiPoints(ByteBuffer byteBuffer)
            throws IOException
    {
        byteBuffer.position(byteBuffer.position() + 4 * DOUBLE_LENGTH);
        int numPoints = byteBuffer.getInt();
        CoordinateSequence coordinateSequence = readCoordinates(byteBuffer, numPoints);
        MultiPoint multiPoint = geometryFactory.createMultiPoint(coordinateSequence);
        return multiPoint;
    }


    /**
     * This is for parsing records with token type = 5(Polygon). It will return a Polygon object with a MBR as bounding box.
     * @param byteBuffer
     * @return
     * @throws IOException
     * @throws ShapeTypeNotMatchException
     */
    public static MultiPolygon parsePolygon(ByteBuffer byteBuffer)
            throws IOException
    {
        byteBuffer.position(byteBuffer.position() + 4 * DOUBLE_LENGTH);
        int numRings = byteBuffer.getInt();
        int numPoints = byteBuffer.getInt();
        int[] ringsOffsets = new int[numRings+1];
        for(int i = 0;i < numRings; ++i){
            ringsOffsets[i] = byteBuffer.getInt();
        }
        ringsOffsets[numRings] = numPoints;

        //read all points out
        CoordinateSequence coordinateSequence = readCoordinates(byteBuffer, numPoints);

        List<LinearRing> holes = new ArrayList<LinearRing>();
        List<LinearRing> shells = new ArrayList<LinearRing>();

        // reading coordinates and assign to holes or shells
        for(int i = 0;i < numRings; ++i){
            int readScale = ringsOffsets[i+1] - ringsOffsets[i];
            CoordinateSequence csRing = geometryFactory.getCoordinateSequenceFactory().create(readScale,2);
            for(int j = 0;j < readScale; ++j){
                csRing.setOrdinate(j, 0, coordinateSequence.getOrdinate(ringsOffsets[i]+j, 0));
                csRing.setOrdinate(j, 1, coordinateSequence.getOrdinate(ringsOffsets[i]+j, 1));
            }
            if(csRing.size() < 3) continue; // if points less than 3, it's not a ring, we just abandon it
            LinearRing ring = geometryFactory.createLinearRing(csRing);
            if(CoordinateSequences.isCCW(csRing)){// is a hole
                holes.add(ring);
            }else{// current coordinate sequance is a
                shells.add(ring);
            }
        }
        // assign shells and holes to polygons
        Polygon[] polygons = null;
        //if only one shell, assign all holes to it directly. If there is no holes, we set each shell as polygon
        if(shells.size() == 1){
            polygons = new Polygon[]{
                    geometryFactory.createPolygon(shells.get(0), GeometryFactory.toLinearRingArray(holes))
            };
        }else if(shells.size() == 0 && holes.size() == 1){// if no shell, we set all holes as shell
            polygons = new Polygon[]{
                    geometryFactory.createPolygon(holes.get(0))
            };
        } else{// there are multiple shells and one or more holes, find which shell a hole is within
            List<ArrayList<LinearRing>> holesWithinShells = new ArrayList<ArrayList<LinearRing>>();
            for(int i = 0;i < shells.size(); ++i){
                holesWithinShells.add(new ArrayList<LinearRing>());
            }
            // for every hole, find home
            for(LinearRing hole : holes){
                //prepare test objects
                LinearRing testRing = hole;
                LinearRing minShell = null;
                Envelope minEnv = null;
                Envelope testEnv = testRing.getEnvelopeInternal();
                Coordinate testPt = testRing.getCoordinateN(0);
                LinearRing tryRing;

                for(LinearRing shell : shells){
                    tryRing = shell;
                    Envelope tryEnv = tryRing.getEnvelopeInternal();
                    if (minShell != null) {
                        minEnv = minShell.getEnvelopeInternal();
                    }
                    boolean isContained = false;
                    Coordinate[] coordList = tryRing.getCoordinates();
                    if (tryEnv.contains(testEnv)
                            && (CGAlgorithms.isPointInRing(testPt, coordList) || (pointInList(
                            testPt, coordList)))) {
                        isContained = true;
                    }
                    if (isContained) {
                        if ((minShell == null) || minEnv.contains(tryEnv)) {
                            minShell = tryRing;
                        }
                    }
                }
                if(minShell == null){// no shell contains this hole, set it to shell
                    shells.add(hole);
                    holesWithinShells.add(new ArrayList<LinearRing>());
                }else{// contained by a shell, assign hole to it
                    holesWithinShells.get(shells.indexOf(minShell)).add(hole);
                }
            }
            // create multi polygon
            polygons = new Polygon[shells.size()];
            for(int i = 0;i < shells.size(); ++i){
                polygons[i] = geometryFactory.createPolygon(shells.get(i),
                        GeometryFactory.toLinearRingArray(holesWithinShells.get(i)));
            }
        }
        return geometryFactory.createMultiPolygon(polygons);
    }

    /**
     * returns true if testPoint is a point in the pointList list.
     * @param testPoint
     * @param pointList
     * @return
     */
    static boolean pointInList(Coordinate testPoint, Coordinate[] pointList) {
        Coordinate p;

        for (int t = pointList.length - 1; t >= 0; t--) {
            p = pointList[t];

            // nan test; x!=x iff x is nan
            if ((testPoint.x == p.x)
                    && (testPoint.y == p.y)
                    && ((testPoint.z == p.z) || (!(testPoint.z == testPoint.z)))
                    ) {
                return true;
            }
        }

        return false;
    }


    /**
     * This is for parsing objects with shape type = PolyLine
     * @param byteBuffer
     * @return
     */
    public static MultiLineString parsePolyLine(ByteBuffer byteBuffer)
            throws IOException
    {
        byteBuffer.position(byteBuffer.position() + 4 * DOUBLE_LENGTH);
        int numParts = byteBuffer.getInt();
        int numPoints = byteBuffer.getInt();
        int[] stringOffsets = new int[numParts+1];
        for(int i = 0;i < numParts; ++i){
            stringOffsets[i] = byteBuffer.getInt();
        }
        CoordinateSequence coordinateSequence = readCoordinates(byteBuffer, numPoints);
        stringOffsets[numParts] = numPoints;
        LineString[] lines = new LineString[numParts];
        for(int i = 0;i < numParts; ++i){
            int readScale = stringOffsets[i+1] - stringOffsets[i];
            CoordinateSequence csString = geometryFactory.getCoordinateSequenceFactory().create(readScale,2);
            for(int j = 0;j < readScale; ++j){
                csString.setOrdinate(j, 0, coordinateSequence.getOrdinate(stringOffsets[i]+j, 0));
                csString.setOrdinate(j, 1, coordinateSequence.getOrdinate(stringOffsets[i]+j, 1));
            }
            lines[i] = geometryFactory.createLineString(csString);
        }
        return geometryFactory.createMultiLineString(lines);
    }
    /**
     * This is for parsing bounding box for shapes with Box.
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static LinearRing parseBoundingBox(DataInputStream inputStream)
            throws IOException
    {
        double xMin = EndianUtils.swapDouble(inputStream.readDouble());
        double yMin = EndianUtils.swapDouble(inputStream.readDouble());
        double xMax = EndianUtils.swapDouble(inputStream.readDouble());
        double yMax = EndianUtils.swapDouble(inputStream.readDouble());

        Coordinate[] boundArray = {
                new Coordinate(xMin, yMin),
                new Coordinate(xMin, yMax),
                new Coordinate(xMax, yMax),
                new Coordinate(xMax, yMin),
                new Coordinate(xMin, yMin)
        };
        return geometryFactory.createLinearRing(boundArray);
    }

    public static Geometry primitiveToShape(byte[] sourceBytes) throws IOException {
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(sourceBytes));
        ByteBuffer byteBuffer = ByteBuffer.wrap(sourceBytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        try{
            validateShapeType(byteBuffer);
        }catch(ShapeTypeNotMatchException e){
            e.printStackTrace();
        }
        switch(currentTokenType){
            case 1:{
                return parsePoint(byteBuffer);
            }
            case 3:{
                return parsePolyLine(byteBuffer);
            }
            case 5:{
                return parsePolygon(byteBuffer);
            }
            case 8:{
                return parseMultiPoints(byteBuffer);
            }
            default:{
                return null;
            }
        }
    }


}
