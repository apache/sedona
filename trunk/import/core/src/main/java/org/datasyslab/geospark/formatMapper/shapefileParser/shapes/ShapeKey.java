/*
 * FILE: ShapeKey
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class ShapeKey
        implements Serializable
{

    /**
     * record id
     */
    Long index = 0l;

    public void write(DataOutput dataOutput)
            throws IOException
    {
        dataOutput.writeLong(index);
    }

    public long getIndex()
    {
        return index;
    }

    public void setIndex(long _index)
    {
        index = _index;
    }
}
