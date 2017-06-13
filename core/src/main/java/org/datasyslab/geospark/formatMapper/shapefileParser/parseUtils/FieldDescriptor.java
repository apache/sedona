package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import org.apache.hadoop.io.Text;

import java.io.Serializable;

/**
 * Created by zongsizhang on 6/2/17.
 */
public class FieldDescriptor implements Serializable{

    /** field name */
    private Text filedName = null;

    /** field type */
    private byte fieldType = 0;

    /** field length */
    private int fieldLength = 0;

    /** decimal count */
    private byte fieldDecimalCount = 0;

    public Text getFiledName() {
        return filedName;
    }

    public void setFiledName(Text filedName) {
        this.filedName = filedName;
    }

    public byte getFieldType() {
        return fieldType;
    }

    public void setFieldType(byte fieldType) {
        this.fieldType = fieldType;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    public byte getFieldDecimalCount() {
        return fieldDecimalCount;
    }

    public void setFieldDecimalCount(byte fieldDecimalCount) {
        this.fieldDecimalCount = fieldDecimalCount;
    }
}
