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

import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Geometry;

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
     * The indexes of geoms that are already part of a cluster
     */
    Set<Integer> isInCluster;

    /**
     * Instantiates a new geometry knn judgement.
     *
     * @param eps the distance eps
     * @param minPoints minimum number of points to form a cluster
     */
    public DBScanJudgement(double eps, int minPoints, Set<Integer> isInCluster)
    {
        this.eps = eps;
        this.minPoints = minPoints;
        this.isInCluster = isInCluster;
    }



    @Override
    public Iterator<Integer> call(Iterator<T> input) throws Exception {
        List<T> geoms = new ArrayList<>();
        while (input.hasNext()) {
            geoms.add(input.next());
        }
        if (geoms.size() <= minPoints) {
            return Collections.emptyIterator();
        }
        Set<Integer> isInCore = new HashSet<>();
        List<Integer> neighbors = new ArrayList<>();
        UnionFind unionFind = new UnionFindImpl(geoms.size());
        for (int i = 0; i < geoms.size(); i++) {
            List<Integer> geomsInEnvelope = getGeomsInEnvelope(geoms, i, eps);
            for (Integer j : geomsInEnvelope) {
                if (neighbors.size() >= minPoints) {
                    if (unionFind.find(i) == unionFind.find(j)) {
                        continue;
                    }
                    if (isInCluster.contains(j) && !isInCore.contains(j)) {
                        continue;
                    }
                }
                double minDistance = geoms.get(i).distance(geoms.get(j));
                if (minDistance <= eps) {
                    if (neighbors.size() < minPoints) {
                        neighbors.add(j);
                        if (neighbors.size() == minPoints) {
                            isInCore.add(i);
                            isInCluster.add(i);
                            for (Integer neighbor : neighbors) {
                                unionIfAvailable(unionFind, i, neighbor, isInCore, isInCluster);
                            }
                        }
                    } else {
                        unionIfAvailable(unionFind, i, j, isInCore, isInCluster);
                    }
                }
            }
        }
        return Arrays.stream(unionFind.getCollapsedClusterIds(isInCluster)).iterator();
    }


    private List<Integer> getGeomsInEnvelope(List<T> geoms, int i, double eps)
    {
        List<Integer> geomsInEnvelope = new ArrayList<>();
        T queryPoint = geoms.get(i);
        for (int j = 0; j < geoms.size(); j++) {
            T geom = geoms.get(j);
            double distance = queryPoint.distance(geom);
            if (i != j && distance <= eps) {
                geomsInEnvelope.add(j);
            }
        }
        return geomsInEnvelope;
    }

    private void unionIfAvailable(UnionFind unionFind, int p, int q, Set<Integer> isInCore, Set<Integer> isInCluster)
    {
        if (isInCluster.contains(q)) {
            if (isInCore.contains(q)) {
                unionFind.union(p, q);
            }
        } else {
            unionFind.union(p, q);
            isInCluster.add(q);
        }
    }
}
