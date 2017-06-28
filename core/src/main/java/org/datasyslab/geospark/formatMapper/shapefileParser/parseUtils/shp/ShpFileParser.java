/**
 * FILE: ShpFileParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShpFileParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import org.apache.commons.io.EndianUtils;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

public class ShpFileParser implements Serializable, ShapeFileConst{

    /** shape type of current .shp file */
    public int currentTokenType = 0;

    /** lenth of file in bytes */
    public long fileLength = 0;

    /** remain length of bytes to parse */
    public long remainLength = 0;

    /** input reader */
    ShapeReader reader = null;

    /**
     * create a new shape file parser with a input source that is instance of DataInputStream
     * @param inputStream
     */
    public ShpFileParser(DataInputStream inputStream) {
        reader = new DataInputStreamReader(inputStream);
    }

    /**
     * extract and validate information from .shp file header
     * @throws IOException
     */
    public void parseShapeFileHead()
            throws IOException
    {
        int fileCode = reader.readInt();
        reader.skip(HEAD_EMPTY_NUM * INT_LENGTH);
        fileLength = 16 * reader.readInt() - HEAD_FILE_LENGTH_16BIT * 16;
        remainLength = fileLength;
        int fileVersion = EndianUtils.swapInteger(reader.readInt());
        currentTokenType = EndianUtils.swapInteger(reader.readInt());
        reader.skip(HEAD_BOX_NUM * DOUBLE_LENGTH);
    }

    /**
     * abstract information from record header and then copy primitive bytes data of record to a primitive record.
     * @return
     * @throws IOException
     */
    public ShpRecord parseRecordPrimitiveContent() throws IOException{
        // get length of record content
        int contentLength = reader.readInt();
        long recordLength = 16 * (contentLength + 4);
        remainLength -= recordLength;
        int typeID = EndianUtils.swapInteger(reader.readInt());
        byte[] contentArray = new byte[contentLength * 2 - INT_LENGTH];// exclude the 4 bytes we read for shape type
        reader.read(contentArray,0,contentArray.length);
        return new ShpRecord(contentArray, typeID);
    }

    /**
     * abstract id number from record header
     * @return
     * @throws IOException
     */
    public int parseRecordHeadID() throws IOException{
        int id = reader.readInt();
        return id;
    }

    /**
     * get current progress of parsing records.
     * @return
     */
    public float getProgress(){
        return 1 - (float)remainLength / (float) fileLength;
    }

}
