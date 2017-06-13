package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import com.vividsolutions.jts.geom.*;
import org.apache.commons.io.EndianUtils;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
        System.out.println();

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

    public static int parseRecordHeadID(DataInputStream inputStream) throws IOException{
        int id = inputStream.readInt();
        return id;
    }

    public static void validateShapeType(DataInputStream inputStream)
            throws IOException, ShapeTypeNotMatchException
    {
        int recordTokenType = EndianUtils.swapInteger(inputStream.readInt());
        if(recordTokenType != currentTokenType) throw new ShapeTypeNotMatchException();
    }

    public static Point parsePoint(DataInputStream inputStream)
            throws IOException
    {
        double x = EndianUtils.swapDouble(inputStream.readDouble());
        double y = EndianUtils.swapDouble(inputStream.readDouble());
//        byte[] bytesx = new byte[DOUBLE_LENGTH];
//        inputStream.readFully(bytesx);
//        double x = EndianUtils.swapDouble(ByteBuffer.wrap(bytesx).getDouble());
//        inputStream.readFully(bytesx);
//        double y = ByteBuffer.wrap(bytesx).getDouble();
        Point point = geometryFactory.createPoint(new Coordinate(x, y));
        return point;
    }


    /**
     * This is for parsing records with token type = 5(Polygon). It will return a Polygon object with a MBR as bounding box.
     * @param inputStream
     * @return
     * @throws IOException
     * @throws ShapeTypeNotMatchException
     */
    public static MultiPolygon parsePolygon(DataInputStream inputStream)
            throws IOException
    {
        LinearRing boundBox = parseBoundingBox(inputStream);
        int numRings = EndianUtils.swapInteger(inputStream.readInt());
        int numPoints = EndianUtils.swapInteger(inputStream.readInt());
        int[] ringsOffsets = new int[numRings+1];
        for(int i = 0;i < numRings; ++i){
            ringsOffsets[i] = EndianUtils.swapInteger(inputStream.readInt());
        }
        ringsOffsets[numRings] = numPoints;

        List<LinearRing> holes = new ArrayList<LinearRing>();
        List<LinearRing> shells = new ArrayList<LinearRing>();

        // reading coordinates and assign to holes or shells
        for(int i = 0;i < numRings; ++i){
            int readScale = ringsOffsets[i+1] - ringsOffsets[i];
            Coordinate[] coordinates = new Coordinate[readScale];
            for(int j = 0;j < readScale; ++j){
                coordinates[j] = parsePoint(inputStream).getCoordinate();
            }
            if(coordinates.length < 3) continue; // if points less than 3, it's not a ring, we just abandon it
            LinearRing ring = geometryFactory.createLinearRing(coordinates);
            CoordinateSequence csRing = geometryFactory.getCoordinateSequenceFactory().create(coordinates);
            if(coordinates[0].x == -99.602176){
                int k = 0;
            }
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
                LinearRing minShell = null;
                for(LinearRing shell : shells){
                    if(shell.contains(hole) && ( minShell == null || minShell.contains(shell) )) minShell = shell;
                }
                if(minShell == null){// no shell contains this hole, set it to shell
                    shells.add(hole);
                    holesWithinShells.add(new ArrayList<LinearRing>());
                }else{// contained by a shell, assign hole to it
                    holesWithinShells.get(holesWithinShells.indexOf(minShell)).add(hole);
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
     * This is for parsing objects with shape type = MultiPoint
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static MultiPoint parseMultiPoints(DataInputStream inputStream)
            throws IOException
    {
        LinearRing boundBox = parseBoundingBox(inputStream);
        int numPoints = EndianUtils.swapInteger(inputStream.readInt());
        Point[] points = new Point[numPoints];
        for(int i = 0;i < numPoints; ++i){
            points[i] = parsePoint(inputStream);
        }
        MultiPoint multiPoint = geometryFactory.createMultiPoint(points);
        return multiPoint;
    }

    /**
     * This is for parsing objects with shape type = PolyLine
     * @param inputStream
     * @return
     */
    public static MultiLineString parsePolyLine(DataInputStream inputStream)
    throws IOException
    {
        LinearRing boundBox = parseBoundingBox(inputStream);
        int numParts = EndianUtils.swapInteger(inputStream.readInt());
        int numPoints = EndianUtils.swapInteger(inputStream.readInt());
        int[] stringOffsets = new int[numParts+1];
        for(int i = 0;i < numParts; ++i){
            stringOffsets[i] = EndianUtils.swapInteger(inputStream.readInt());
        }
        stringOffsets[numParts] = numPoints;
        LineString[] lines = new LineString[numParts];
        for(int i = 0;i < numParts; ++i){
            int readScale = stringOffsets[i+1] - stringOffsets[i];
            Coordinate[] coordinates = new Coordinate[readScale];
            for(int j = 0;j < readScale; ++j){
                coordinates[j] = parsePoint(inputStream).getCoordinate();
            }
            lines[i] = geometryFactory.createLineString(coordinates);
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
        try{
            validateShapeType(inputStream);
        }catch(ShapeTypeNotMatchException e){
            e.printStackTrace();
        }
        switch(currentTokenType){
            case 1:{
                return parsePoint(inputStream);
            }
            case 3:{
                return parsePolyLine(inputStream);
            }
            case 5:{
                return parsePolygon(inputStream);
            }
            case 8:{
                return parseMultiPoints(inputStream);
            }
            default:{
                return null;
            }
        }
    }


}
