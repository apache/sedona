/*
 * FILE: IndexBuilder
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

package org.datasyslab.geospark.spatioTemporalRddTool;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.SpatioTemporalObjects.SpatioTemporalObject;
import org.datasyslab.geospark.enums.IndexType;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

public final class SpatioTemporalIndexBuilder<T extends SpatioTemporalObject>
        implements FlatMapFunction<Iterator<T>, SpatialIndex>
{
    IndexType indexType;

    public SpatioTemporalIndexBuilder(IndexType indexType)
    {
        this.indexType = indexType;
    }

    @Override
    public Iterator<SpatialIndex> call(Iterator<T> objectIterator)
            throws Exception
    {
        SpatialIndex spatialIndex;
        if (indexType == IndexType.RTREE) {
            spatialIndex = new STRtree();
        }
        else if (indexType == indexType.STRTREE) {
            spatialIndex = new STRtree();
        }
        else {
            spatialIndex = new Quadtree();
        }
        while (objectIterator.hasNext()) {
            T spatialObject = objectIterator.next();
            spatialIndex.insert(spatialObject.getEnvelopeInternal(), spatialObject);
        }
        Set<SpatialIndex> result = new HashSet();
        spatialIndex.query(new Envelope(0.0, 0.0, 0.0, 0.0));
        result.add(spatialIndex);
        return result.iterator();
    }
}
