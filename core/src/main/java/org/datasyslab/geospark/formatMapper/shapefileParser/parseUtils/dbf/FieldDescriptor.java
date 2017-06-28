/**
 * FILE: FieldDescriptor.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf;

import java.io.Serializable;

public class FieldDescriptor implements Serializable{

    /** field name */
    private String filedName = null;

    /** field type */
    private byte fieldType = 0;

    /** field length */
    private int fieldLength = 0;

    /** decimal count */
    private byte fieldDecimalCount = 0;

    public String getFiledName() {
        return filedName;
    }

    public void setFiledName(String filedName) {
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
