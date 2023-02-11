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

import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.ShpRecord;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class ShpFileParser
        implements Serializable, ShapeFileConst
{

    /**
     * input reader
     */
    private final SafeReader reader;
    /**
     * length of file in bytes
     */
    private long fileLength = 0;
    /**
     * remain length of bytes to parse
     */
    private long remainLength = 0;

    /**
     * create a new shape file parser with a input source that is instance of DataInputStream
     *
     * @param inputStream
     */
    public ShpFileParser(FSDataInputStream inputStream)
    {
        reader = new SafeReader(inputStream);
    }

    /**
     * extract and validate information from .shp file header
     *
     * @throws IOException
     */
    public void parseShapeFileHead()
            throws IOException
    {
        reader.skip(INT_LENGTH);
        reader.skip(HEAD_EMPTY_NUM * INT_LENGTH);
        fileLength = 2 * ((long) reader.readInt() - HEAD_FILE_LENGTH_16BIT);
        remainLength = fileLength;
        // Skip 2 integers: file version and token type
        reader.skip(2 * INT_LENGTH);
        // if bound box is not referenced, skip it
        reader.skip(HEAD_BOX_NUM * DOUBLE_LENGTH);
    }

    /**
     * abstract information from record header and then copy primitive bytes data of record to a primitive record.
     *
     * @return
     * @throws IOException
     */
    public ShpRecord parseRecordPrimitiveContent()
            throws IOException
    {
        // get length of record content
        int contentLength = reader.readInt();
        long recordLength = 2 * (contentLength + 4);
        remainLength -= recordLength;
        int typeID = EndianUtils.swapInteger(reader.readInt());
        byte[] contentArray = new byte[contentLength * 2 - INT_LENGTH];// exclude the 4 bytes we read for shape type
        reader.read(contentArray, 0, contentArray.length);
        return new ShpRecord(contentArray, typeID);
    }

    /**
     * abstract information from record header and then copy primitive bytes data of record to a primitive record.
     *
     * @return
     * @throws IOException
     */
    public ShpRecord parseRecordPrimitiveContent(int length)
            throws IOException
    {
        // get length of record content
        int contentLength = reader.readInt();
        long recordLength = 2 * (contentLength + 4);
        remainLength -= recordLength;
        int typeID = EndianUtils.swapInteger(reader.readInt());
        byte[] contentArray = new byte[length];// exclude the 4 bytes we read for shape type
        reader.read(contentArray, 0, contentArray.length);
        return new ShpRecord(contentArray, typeID);
    }

    /**
     * abstract id number from record header
     *
     * @return
     * @throws IOException
     */
    public int parseRecordHeadID()
            throws IOException
    {
        return reader.readInt();
    }

    /**
     * get current progress of parsing records.
     *
     * @return
     */
    public float getProgress()
    {
        return 1 - (float) remainLength / (float) fileLength;
    }

    /**
     * A limited wrapper around FSDataInputStream providing proper implementations
     * for methods of FSDataInputStream which are known to be broken on some platforms.
     */
    private static final class SafeReader
    {
        private final FSDataInputStream input;

        private SafeReader(FSDataInputStream input)
        {
            this.input = input;
        }

        public int readInt()
                throws IOException
        {
            byte[] bytes = new byte[INT_LENGTH];
            input.readFully(bytes);
            return ByteBuffer.wrap(bytes).getInt();
        }

        public void skip(int numBytes)
                throws IOException
        {
            input.skip(numBytes);
        }

        public void read(byte[] buffer, int offset, int length)
                throws IOException
        {
            input.readFully(buffer, offset, length);
        }
    }
}
