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
