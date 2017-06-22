package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf;

import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.io.Text;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeFileConst;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zongsizhang on 5/29/17.
 */
public class DbfParseUtil implements ShapeFileConst {

    /** number of record get from header */
    public int numRecord = 0;

    /** number of bytes per record */
    public int numBytesRecord = 0;

    /** number of records already read. Records that is ignored also counted in */
    public int numRecordRead;

    public boolean isDone(){
        return numRecordRead >= numRecord;
    }

    public float getProgress(){
        return (float)numRecordRead / (float)numRecord;
    }

    /**
     * parse header of .dbf file and draw information for next step
     * @param inputStream
     * @return
     * @throws IOException
     */
    public List<FieldDescriptor> parseFileHead(DataInputStream inputStream) throws IOException {
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
        numBytesRecord =  EndianUtils.swapShort(inputStream.readShort());
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
        List<FieldDescriptor> fieldDescriptors = new ArrayList<>();
        byte terminator = inputStream.readByte();
        while(terminator != FIELD_DESCRIPTOR_TERMINATOR){
            FieldDescriptor descriptor = new FieldDescriptor();
            //read field name
            byte[] nameBytes = new byte[FIELD_NAME_LENGTH];
            nameBytes[0] = terminator;
            inputStream.readFully(nameBytes,1,10);
            int zeroId = 0;
            while(nameBytes[zeroId] != 0) zeroId++;
            Text fieldName = new Text();
            fieldName.append(nameBytes, 0, zeroId);
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
        return fieldDescriptors;
    }

    /**
     * draw raw byte array of effective record
     * @param inputStream
     * @return
     * @throws IOException
     */
    public byte[] parsePrimitiveRecord(DataInputStream inputStream) throws IOException {
        if(isDone()) return null;
        byte flag = inputStream.readByte();
        final int recordLength = numBytesRecord - 1;//exclude skip the record flag when read and skip
        while(flag == RECORD_DELETE_FLAG){
            inputStream.skipBytes(recordLength);
            numRecordRead++;
            flag = inputStream.readByte();
        }
        if(flag == FILE_END_FLAG) return null;
        byte[] primitiveBytes = new byte[recordLength];
        inputStream.readFully(primitiveBytes);
        numRecordRead++; //update number of record read
        return primitiveBytes;
    }

    /**
     * abstract attributes from primitive bytes according to field descriptors.
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static String primitiveToAttributes(DataInputStream inputStream, List<FieldDescriptor> fieldDescriptors)
            throws IOException
    {
        byte[] delimiter = {'\t'};
        Text attributes = new Text();
        for(FieldDescriptor descriptor : fieldDescriptors){
            byte[] fldBytes = new byte[descriptor.getFieldLength()];
            inputStream.readFully(fldBytes);
            attributes.append(fldBytes,0,fldBytes.length);
            attributes.append(delimiter,0,1);
        }
        return attributes.toString();
    }



}
