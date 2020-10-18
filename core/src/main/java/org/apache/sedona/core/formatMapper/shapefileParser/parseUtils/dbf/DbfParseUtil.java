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

package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf;

import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.io.Text;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeFileConst;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DbfParseUtil
        implements ShapeFileConst
{

    /**
     * number of record get from header
     */
    public int numRecord = 0;

    /**
     * number of bytes per record
     */
    public int numBytesRecord = 0;

    /**
     * number of records already read. Records that is ignored also counted in
     */
    public int numRecordRead;
    /**
     * fieldDescriptors of current .dbf file
     */
    private List<FieldDescriptor> fieldDescriptors = null;

    /**
     * Copied from org.geotools.data.shapefile.dbf.fastParse
     * Performs a faster byte[] to String conversion under the assumption the content
     * is represented with one byte per char
     *
     * @param bytes
     * @param fieldOffset
     * @param fieldLen
     * @return
     */
    private static String fastParse(final byte[] bytes, final int fieldOffset, final int fieldLen)
    {
        // faster reading path, the decoder is for some reason slower,
        // probably because it has to make extra checks to support multibyte chars
        final char[] chars = new char[fieldLen];
        for (int i = 0; i < fieldLen; i++) {
            // force the byte to a positive integer interpretation before casting to char
            chars[i] = ((char) (0x00FF & bytes[fieldOffset + i]));
        }
        return new String(chars);
    }

    public boolean isDone()
    {
        return numRecordRead >= numRecord;
    }

    public float getProgress()
    {
        return (float) numRecordRead / (float) numRecord;
    }

    public List<FieldDescriptor> getFieldDescriptors()
    {
        return fieldDescriptors;
    }

    /**
     * parse header of .dbf file and draw information for next step
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    public void parseFileHead(DataInputStream inputStream)
            throws IOException
    {
        // version
        inputStream.readByte();
        // date YYMMDD format
        byte[] date = new byte[3];
        inputStream.readFully(date);
        // number of records in file
        numRecord = EndianUtils.swapInteger(inputStream.readInt());
        // number of bytes in header
        int numBytes = EndianUtils.swapShort(inputStream.readShort());
        // number of bytes in file
        numBytesRecord = EndianUtils.swapShort(inputStream.readShort());
        // skip reserved 2 byte
        inputStream.skipBytes(2);
        // skip flag indicating incomplete transaction
        inputStream.skipBytes(1);
        // skip encryption flag
        inputStream.skipBytes(1);
        // skip reserved 12 bytes for DOS in a multi-user environment
        inputStream.skipBytes(12);
        // skip production .mdx file flag
        inputStream.skipBytes(1);
        // skip language driver id
        inputStream.skipBytes(1);
        // skip reserved 2 bytes
        inputStream.skipBytes(2);
        // parse n filed descriptors
        fieldDescriptors = new ArrayList<>();
        byte terminator = inputStream.readByte();
        while (terminator != FIELD_DESCRIPTOR_TERMINATOR) {
            FieldDescriptor descriptor = new FieldDescriptor();
            //read field name
            byte[] nameBytes = new byte[FIELD_NAME_LENGTH];
            nameBytes[0] = terminator;
            inputStream.readFully(nameBytes, 1, 10);
            int zeroId = 0;
            while (nameBytes[zeroId] != 0) { zeroId++; }
            Text fieldName = new Text();
            fieldName.append(nameBytes, 0, zeroId);
            descriptor.setFieldName(fieldName.toString());
            // read field type
            descriptor.setFieldType(inputStream.readByte());
            // skip reserved field
            inputStream.readInt();
            // read field length
            descriptor.setFieldLength(inputStream.readUnsignedByte());
            // read field decimal count
            descriptor.setFieldDecimalCount(inputStream.readByte());
            // skip the next 14 bytes
            inputStream.skipBytes(14);
            fieldDescriptors.add(descriptor);
            terminator = inputStream.readByte();
        }
    }

    /**
     * draw raw byte array of effective record
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    public String parsePrimitiveRecord(DataInputStream inputStream)
            throws IOException
    {
        if (isDone()) { return null; }
        byte flag = inputStream.readByte();
        final int recordLength = numBytesRecord - 1;//exclude skip the record flag when read and skip
        while (flag == RECORD_DELETE_FLAG) {
            inputStream.skipBytes(recordLength);
            numRecordRead++;
            flag = inputStream.readByte();
        }
        if (flag == FILE_END_FLAG) { return null; }
        byte[] primitiveBytes = new byte[recordLength];
        inputStream.readFully(primitiveBytes);
        numRecordRead++; //update number of record read
        return primitiveToAttributes(ByteBuffer.wrap(primitiveBytes));
    }

    /**
     * abstract attributes from primitive bytes according to field descriptors.
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    public String primitiveToAttributes(DataInputStream inputStream)
            throws IOException
    {
        byte[] delimiter = {'\t'};
        Text attributes = new Text();
        for (int i = 0; i < fieldDescriptors.size(); ++i) {
            FieldDescriptor descriptor = fieldDescriptors.get(i);
            byte[] fldBytes = new byte[descriptor.getFieldLength()];
            inputStream.readFully(fldBytes);
            //System.out.println(descriptor.getFiledName() + "  " + new String(fldBytes));
            byte[] attr = new String(fldBytes).trim().getBytes();
            if (i > 0) {
                attributes.append(delimiter, 0, 1);// first attribute doesn't append '\t'
            }
            attributes.append(attr, 0, attr.length);
        }
        String attrs = attributes.toString();
        return attributes.toString();
    }

    /**
     * abstract attributes from primitive bytes according to field descriptors.
     *
     * @param buffer
     * @return
     * @throws IOException
     */
    public String primitiveToAttributes(ByteBuffer buffer)
            throws IOException
    {
        byte[] delimiter = {'\t'};
        Text attributes = new Text();
        for (int i = 0; i < fieldDescriptors.size(); ++i) {
            FieldDescriptor descriptor = fieldDescriptors.get(i);
            byte[] fldBytes = new byte[descriptor.getFieldLength()];
            buffer.get(fldBytes, 0, fldBytes.length);
            String charset = System.getProperty("sedona.global.charset", "default");
            Boolean utf8flag = charset.equalsIgnoreCase("utf8");
            byte[] attr = utf8flag ? fldBytes : fastParse(fldBytes, 0, fldBytes.length).trim().getBytes();
            if (i > 0) {
                attributes.append(delimiter, 0, 1);// first attribute doesn't append '\t'
            }
            attributes.append(attr, 0, attr.length);
        }
        return attributes.toString();
    }
}
