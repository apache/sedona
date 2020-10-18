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

package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.io.BytesWritable;

import java.io.Serializable;

public class ShpRecord
        implements Serializable
{

    /**
     * primitive byte contents
     */
    private BytesWritable bytes = null;

    /**
     * shape type
     */
    private int typeID = -1;

    /**
     * create a ShpRecord with primitive bytes and shape type id we abstract from .shp file
     *
     * @param byteArray
     * @param shapeTypeID
     */
    public ShpRecord(byte[] byteArray, int shapeTypeID)
    {
        bytes = new BytesWritable();
        bytes.set(byteArray, 0, byteArray.length);
        typeID = shapeTypeID;
    }

    public BytesWritable getBytes()
    {
        return bytes;
    }

    public int getTypeID()
    {
        return typeID;
    }
}
