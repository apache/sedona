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

package org.apache.sedona.core.spatialRddTool;

import org.apache.sedona.core.enums.IndexType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class IndexBuilder<T extends Geometry>
        implements FlatMapFunction<Iterator<T>, SpatialIndex>
{
    IndexType indexType;

    public IndexBuilder(IndexType indexType)
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
