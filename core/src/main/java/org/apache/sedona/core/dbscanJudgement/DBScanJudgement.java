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

package org.apache.sedona.core.dbscanJudgement;

import org.apache.sedona.core.rangeJudgement.JudgementBase;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.geotools.geometry.Envelope2D;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.GeometryItemDistance;
import org.locationtech.jts.index.strtree.STRtree;
import org.opengis.geometry.BoundingBox;

import java.io.Serializable;
import java.util.*;

// TODO: Auto-generated Javadoc

/**
 * The Class KnnJudgementUsingIndex.
 */
public class DBScanJudgement<T extends Geometry>
        implements FlatMapFunction<Iterator<T>, Integer>
{

    /**
     * The eps distance
     */
    double eps;

    /**
     * The minimum number of points to form a cluster
     */
    int minPoints;

    /**
     * Instantiates a new geometry knn judgement.
     *
     * @param eps the distance eps
     * @param minPoints minimum number of points to form a cluster
     */
    public DBScanJudgement(double eps, int minPoints)
    {
        this.eps = eps;
        this.minPoints = minPoints;
    }



    @Override
    public Iterator<Integer> call(Iterator<T> input) throws Exception {
        List<T> geoms = new ArrayList<>();
        while (input.hasNext()) {
            geoms.add(input.next());
        }
        UnionFind unionFind = new UnionFindImpl(geoms.size());
        for (int i = 0; i < geoms.size(); i++) {
            List<Integer> neighbors = getNeighbors(geoms, i, eps);
            if (neighbors.size() >= minPoints) {
                for (Integer neighbor : neighbors) {
                    unionFind.union(i, neighbor);
                }
            }
        }
        return Arrays.stream(unionFind.getCollapsedClusterIds()).iterator();
    }


    private List<Integer> getNeighbors(List<T> geoms, int i, double eps)
    {
        List<Integer> neighbors = new ArrayList<>();
        T queryPoint = geoms.get(i);
        for (int j = 0; j < geoms.size(); j++) {
            T geom = geoms.get(j);
            double distance = queryPoint.distance(geom);
            if (i != j && distance <= eps) {
                neighbors.add(j);
            }
        }
        return neighbors;
    }
}
