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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: Auto-generated Javadoc

/**
 * The Class DBScanJudgement.
 */
public class DBScanJudgement<T extends Geometry>
        implements FlatMapFunction<Iterator<T>, Integer> {

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
     * @param eps       the distance eps
     * @param minPoints the minimum number of points to form a cluster
     */
    public DBScanJudgement(double eps, int minPoints, Set<Integer> isInCluster) {
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
        if (geoms.size() < minPoints) {
            return Collections.emptyIterator();
        }
        Set<Integer> isInCore = new HashSet<>();
        Integer[] neighbors = new Integer[minPoints];
        UnionFind unionFind = new UnionFindImpl(geoms.size());
        STRtree strtree = new STRtree(geoms.size());
        for (Geometry geom : geoms) {
            strtree.insert(geom.getEnvelopeInternal(), geom);
        }
        for (int i = 0; i < geoms.size(); i++) {
            int numNeighbors = 0;
            List<Integer> geomsInEnvelope = getGeomsInEnvelope(geoms, i, eps, strtree);
            if (geomsInEnvelope.size() < minPoints) {
                continue;
            }

            for (Integer j : geomsInEnvelope) {
                if (numNeighbors >= minPoints) {
                    /*
                     * If we've already identified p as a core point, and it's already
                     * in the same cluster in q, then there's nothing to learn by
                     * computing the distance.
                     */
                    if (unionFind.find(i) == unionFind.find(j)) {
                        continue;
                    }
                    /*
                     * Similarly, if q is already identified as a border point of another
                     * cluster, there's no point figuring out what the distance is.
                     */
                    if (isInCluster.contains(j) && !isInCore.contains(j)) {
                        continue;
                    }
                }
                double minDistance = geoms.get(i).distance(geoms.get(j));
                if (minDistance <= eps) {
                    /*
                     * If we haven't hit min_points yet, we don't know if we can perform union of p and q.
                     * Just set q aside for now.
                     */
                    if (numNeighbors < minPoints) {
                        neighbors[numNeighbors++] = j;
                        /*
                         * If we just hit min_points, we can now perform union of all the neighbor geometries
                         * we've been saving.
                         */
                        if (numNeighbors == minPoints) {
                            isInCore.add(i);
                            isInCluster.add(i);
                            for (int k = 0; k < numNeighbors; k++) {
                                unionIfAvailable(unionFind, i, neighbors[k], isInCore, isInCluster);
                            }
                        }
                    } else {
                        /*
                         * If we're above min_points, no need to store our neighbors, just go ahead
                         * and union them now.  This may allow us to cut out some distance computations.
                         */
                        unionIfAvailable(unionFind, i, j, isInCore, isInCluster);
                    }
                }
            }
        }
        Integer[] collapsedClusterIds = unionFind.getCollapsedClusterIds(isInCluster);
        return Arrays.stream(collapsedClusterIds).iterator();
    }


    private List<Integer> getGeomsInEnvelope(List<T> geoms, int i, double eps, STRtree strTree) {
        Map<Geometry, Integer> geomIndexMap = new HashMap<>();
        for (int index = 0; index < geoms.size(); index++) {
            geomIndexMap.put(geoms.get(index), index);
        }

        Envelope envelope = null;
        if (Objects.equals(geoms.get(i).getGeometryType(), Geometry.TYPENAME_POINT)) {
            Point point = (Point) geoms.get(i);
            envelope = new Envelope(point.getX() - eps, point.getX() + eps, point.getY() - eps, point.getY() + eps);
        } else {
            Envelope box = geoms.get(i).getEnvelopeInternal();
            envelope = new Envelope(box.getMinX() - eps, box.getMaxX() + eps, box.getMinY() - eps, box.getMaxY() + eps);
        }

        List<Geometry> geomsInEnvelope = strTree.query(envelope);
        return geomsInEnvelope.stream().map(geomIndexMap::get).collect(Collectors.toList());
    }

    private void unionIfAvailable(UnionFind unionFind, int p, int q, Set<Integer> isInCore, Set<Integer> isInCluster) {
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
