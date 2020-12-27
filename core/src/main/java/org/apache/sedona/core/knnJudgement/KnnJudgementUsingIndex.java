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

package org.apache.sedona.core.knnJudgement;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.GeometryItemDistance;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class KnnJudgementUsingIndex.
 */
public class KnnJudgementUsingIndex<U extends Geometry, T extends Geometry>
        implements FlatMapFunction<Iterator<SpatialIndex>, T>, Serializable
{

    /**
     * The k.
     */
    int k;

    /**
     * The query center.
     */
    U queryCenter;

    /**
     * Instantiates a new knn judgement using index.
     *
     * @param queryCenter the query center
     * @param k the k
     */
    public KnnJudgementUsingIndex(U queryCenter, int k)
    {
        this.queryCenter = queryCenter;
        this.k = k;
    }

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    @Override
    public Iterator<T> call(Iterator<SpatialIndex> treeIndexes)
            throws Exception
    {
        SpatialIndex treeIndex = treeIndexes.next();
        final Object[] localK;
        if (treeIndex instanceof STRtree) {
            localK = ((STRtree) treeIndex).nearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k);
        }
        else {
            throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
        }
        List<T> result = new ArrayList();
        for (int i = 0; i < localK.length; i++) {
            result.add((T) localK[i]);
        }
        return result.iterator();
    }
}
