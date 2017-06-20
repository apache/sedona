package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import org.apache.commons.io.EndianUtils;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.DataInputStreamReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeFileConst;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by zongsizhang on 6/19/17.
 */
public class ShpFileParser implements Serializable, ShapeFileConst{

    /** shape type of current .shp file */
    public int currentTokenType = 0;

    /** lenth of file in bytes */
    public long fileLength = 0;

    /** remain length of bytes to parse */
    public long remainLength = 0;

    /** input reader */
    ShapeReader reader = null;

    public ShpFileParser(DataInputStream inputStream) {
        reader = new DataInputStreamReader(inputStream);
    }

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

    public int parseRecordHeadID() throws IOException{
        int id = reader.readInt();
        return id;
    }

    public float getProgress(){
        return 1 - (float)remainLength / (float) fileLength;
    }

}
