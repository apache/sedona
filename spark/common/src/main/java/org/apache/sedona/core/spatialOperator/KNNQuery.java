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
package org.apache.sedona.core.spatialOperator;

import java.io.Serializable;
import java.util.List;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.core.knnJudgement.GeometryDistanceComparator;
import org.apache.sedona.core.knnJudgement.KnnJudgement;
import org.apache.sedona.core.knnJudgement.KnnJudgementUsingIndex;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.locationtech.jts.geom.Geometry;

// TODO: Auto-generated Javadoc

/** The Class KNNQuery. */
public class KNNQuery implements Serializable {

  /**
   * Spatial knn query.
   *
   * @param spatialRDD the spatial RDD
   * @param originalQueryPoint the original query window
   * @param k the k
   * @param useIndex the use index
   * @return the list
   */
  public static <U extends Geometry, T extends Geometry> List<T> SpatialKnnQuery(
      SpatialRDD<T> spatialRDD, U originalQueryPoint, Integer k, boolean useIndex) {
    U queryCenter = originalQueryPoint;
    if (spatialRDD.getCRStransformation()) {
      try {
        queryCenter =
            (U)
                FunctionsGeoTools.transform(
                    originalQueryPoint,
                    spatialRDD.getSourceEpsgCode(),
                    spatialRDD.getTargetEpgsgCode());
      } catch (FactoryException | TransformException e) {
        throw new RuntimeException(e);
      }
    }

    if (useIndex) {
      if (spatialRDD.indexedRawRDD == null) {
        throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
      }
      JavaRDD<T> tmp =
          spatialRDD.indexedRawRDD.mapPartitions(new KnnJudgementUsingIndex(queryCenter, k));
      List<T> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter, true));
      // Take the top k
      return result;
    } else {
      JavaRDD<T> tmp =
          spatialRDD.getRawSpatialRDD().mapPartitions(new KnnJudgement(queryCenter, k));
      List<T> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter, true));
      // Take the top k
      return result;
    }
  }
}
