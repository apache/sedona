/*
 * FILE: ShpRecord
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
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

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
