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
package org.apache.sedona.common.utils;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * A utility class for finding CRS transforms. It avoids the expensive equals() call on CRS objects
 * in the CRS.findTransform method.
 */
public class CachedCRSTransformFinder {
  private CachedCRSTransformFinder() {}

  private static class CRSPair {
    private final CoordinateReferenceSystem sourceCRS;
    private final CoordinateReferenceSystem targetCRS;
    private final int hashCode;

    public CRSPair(CoordinateReferenceSystem sourceCRS, CoordinateReferenceSystem targetCRS) {
      this.sourceCRS = sourceCRS;
      this.targetCRS = targetCRS;
      this.hashCode = sourceCRS.hashCode() * 31 + targetCRS.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CRSPair)) {
        return false;
      }
      CRSPair other = (CRSPair) obj;
      // Here we use == instead of equals deliberately because CRS objects will be heavily reused
      // throughout the system. Different rasters with same CRS definition will have a high
      // probability
      // of using the same CRS object.
      //
      // There are cases where two CRS objects are equal but not the same object. In this case, the
      // CRS.findTransform method will look up its internal cache using the equals() method, which
      // is pretty
      // slow. This CachedCRSTransformFinder will accelerate the cases where there are multiple (but
      // not too many)
      // equivalent CRS objects. Typically, there could be 2 equivalent CRS objects: one created
      // from looking up
      // the EPSG database, and the other created from deserializing the CRS object.
      return sourceCRS == other.sourceCRS && targetCRS == other.targetCRS;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private static final LoadingCache<CRSPair, MathTransform> crsTransformCache =
      Caffeine.newBuilder().maximumSize(1000).build(CachedCRSTransformFinder::doFindTransform);

  public static MathTransform findTransform(
      CoordinateReferenceSystem sourceCRS, CoordinateReferenceSystem targetCRS) {
    return crsTransformCache.get(new CRSPair(sourceCRS, targetCRS));
  }

  private static MathTransform doFindTransform(CRSPair crsPair) throws FactoryException {
    return CRS.findMathTransform(crsPair.sourceCRS, crsPair.targetCRS, true);
  }
}
