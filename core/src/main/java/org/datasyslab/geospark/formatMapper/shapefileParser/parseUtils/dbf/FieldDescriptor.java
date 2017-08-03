/**
 * FILE: FieldDescriptor.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class FieldDescriptor.
 */
public class FieldDescriptor implements Serializable{

    /**  field name. */
    private String filedName = null;

    /**  field type. */
    private byte fieldType = 0;

    /**  field length. */
    private int fieldLength = 0;

    /**  decimal count. */
    private byte fieldDecimalCount = 0;

    /**
     * Gets the filed name.
     *
     * @return the filed name
     */
    public String getFiledName() {
        return filedName;
    }

    /**
     * Sets the filed name.
     *
     * @param filedName the new filed name
     */
    public void setFiledName(String filedName) {
        this.filedName = filedName;
    }

    /**
     * Gets the field type.
     *
     * @return the field type
     */
    public byte getFieldType() {
        return fieldType;
    }

    /**
     * Sets the field type.
     *
     * @param fieldType the new field type
     */
    public void setFieldType(byte fieldType) {
        this.fieldType = fieldType;
    }

    /**
     * Gets the field length.
     *
     * @return the field length
     */
    public int getFieldLength() {
        return fieldLength;
    }

    /**
     * Sets the field length.
     *
     * @param fieldLength the new field length
     */
    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    /**
     * Gets the field decimal count.
     *
     * @return the field decimal count
     */
    public byte getFieldDecimalCount() {
        return fieldDecimalCount;
    }

    /**
     * Sets the field decimal count.
     *
     * @param fieldDecimalCount the new field decimal count
     */
    public void setFieldDecimalCount(byte fieldDecimalCount) {
        this.fieldDecimalCount = fieldDecimalCount;
    }
}
