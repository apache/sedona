/*
 * FILE: FieldDescriptor
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Class FieldDescriptor.
 */
public class FieldDescriptor
        implements Serializable
{

    /**
     * field name.
     */
    private String fieldName = null;

    /**
     * field type.
     */
    private byte fieldType = 0;

    /**
     * field length.
     */
    private int fieldLength = 0;

    /**
     * decimal count.
     */
    private byte fieldDecimalCount = 0;

    /**
     * Gets the field name.
     *
     * @return the field name
     */
    public String getFieldName()
    {
        return fieldName;
    }

    /**
     * Sets the field name.
     *
     * @param fieldName the new field name
     */
    public void setFieldName(String fieldName)
    {
        this.fieldName = fieldName;
    }

    /**
     * Gets the field type.
     *
     * @return the field type
     */
    public byte getFieldType()
    {
        return fieldType;
    }

    /**
     * Sets the field type.
     *
     * @param fieldType the new field type
     */
    public void setFieldType(byte fieldType)
    {
        this.fieldType = fieldType;
    }

    /**
     * Gets the field length.
     *
     * @return the field length
     */
    public int getFieldLength()
    {
        return fieldLength;
    }

    /**
     * Sets the field length.
     *
     * @param fieldLength the new field length
     */
    public void setFieldLength(int fieldLength)
    {
        this.fieldLength = fieldLength;
    }

    /**
     * Gets the field decimal count.
     *
     * @return the field decimal count
     */
    public byte getFieldDecimalCount()
    {
        return fieldDecimalCount;
    }

    /**
     * Sets the field decimal count.
     *
     * @param fieldDecimalCount the new field decimal count
     */
    public void setFieldDecimalCount(byte fieldDecimalCount)
    {
        this.fieldDecimalCount = fieldDecimalCount;
    }
}
