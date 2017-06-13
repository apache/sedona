package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.io.Text;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeFileConst;
import scala.Char;

import java.io.DataInputStream;
import java.io.FileDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zongsizhang on 5/29/17.
 */
public class DbfParseUtil implements ShapeFileConst {

    public static class DbfInfoBundle{
        /** number of record in current .dbf file */
        public int numRecord = 0;

        /** number of bytes in current .dbf file */
        public int numBytesRecord = 0;

        /** List of fieldDescriptor */
        public List<FieldDescriptor> fieldDescriptors = null;

        /** num of records read */
        public int numRecordRead = 0;

        public boolean isDone(){
            return numRecordRead >= numRecord;
        }

        public float getProgress(){
            return (float)numRecordRead / (float)numRecord;
        }

    }

    public static DbfInfoBundle infoBundle = null;

    public static void renewParser(){
        infoBundle = null;
    }

    public static DbfInfoBundle parseFileHead(DataInputStream inputStream) throws IOException {
        //create info bundle
        infoBundle = new DbfInfoBundle();
        // version
        inputStream.readByte();
        // date YYMMDD format
        byte[] date = new byte[3];
        inputStream.readFully(date);
        // number of records in file
        infoBundle.numRecord = EndianUtils.swapInteger(inputStream.readInt());
        // number of bytes in header
        int numBytes = EndianUtils.swapShort(inputStream.readShort());
        // number of bytes in file
        infoBundle.numBytesRecord =  EndianUtils.swapShort(inputStream.readShort());
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
        byte terminator = inputStream.readByte();
        infoBundle.fieldDescriptors = new ArrayList<FieldDescriptor>();
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
            infoBundle.fieldDescriptors.add(descriptor);
            terminator = inputStream.readByte();
        }
        return infoBundle;
    }

    public static byte[] parsePrimitiveRecord(DataInputStream inputStream, DbfInfoBundle dbfInfo) throws IOException {
        if(dbfInfo.isDone()) return null;
        byte flag = inputStream.readByte();
        final int recordLength = dbfInfo.numBytesRecord - 1;//exclude skip the record flag when read and skip
        while(flag == RECORD_DELETE_FLAG){
            inputStream.skipBytes(recordLength);
            dbfInfo.numRecordRead++;
            flag = inputStream.readByte();
        }
        if(flag == FILE_END_FLAG) return null;
        byte[] primitiveBytes = new byte[recordLength];
        inputStream.readFully(primitiveBytes);
        dbfInfo.numRecordRead++; //update number of record read
        return primitiveBytes;
    }

    public static String primitiveToAttributes(DataInputStream inputStream) throws IOException {
        byte[] delimiter = {'\t'};
        Text attributes = new Text();
        for(FieldDescriptor descriptor : DbfParseUtil.infoBundle.fieldDescriptors){
            byte[] fldBytes = new byte[descriptor.getFieldLength()];
            inputStream.readFully(fldBytes);
            switch(descriptor.getFieldType()){
                case 'C':
                {
                    attributes.append(fldBytes,0,fldBytes.length);
                    attributes.append(delimiter,0,1);
                    break;
                }
                case 'N' | 'F':
                {
                    attributes.append(fldBytes,0,fldBytes.length);
                    attributes.append(delimiter,0,1);
                    break;
                }
                case 'D':{
                    attributes.append(fldBytes,0,fldBytes.length);
                    attributes.append(delimiter,0,1);
                    break;
                }
            }
        }
        return attributes.toString();
    }



}
