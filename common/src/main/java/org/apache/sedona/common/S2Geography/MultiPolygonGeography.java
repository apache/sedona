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
package org.apache.sedona.common.S2Geography;

import com.google.common.geometry.S2Polygon;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MultiPolygonGeography extends GeographyCollection {
  /**
   * Wrap each raw S2Polygon in a PolygonGeography, then hand it off to GeographyCollection to do
   * the rest (including serialization).
   */
  public MultiPolygonGeography(List<S2Polygon> polygons) {
    super(
        polygons.stream()
            .map(PolygonGeography::new) // wrap each S2Polygon
            .collect(Collectors.toList())); // into a List<PolygonGeography>
    if (polygons.isEmpty()) {
      new MultiPolygonGeography();
    }
  }

  public MultiPolygonGeography() {
    super(Collections.emptyList());
  }

  public List<S2Geography> getFeatures() {
    return features;
  }

  @Override
  public int dimension() {
    // every child PolygonGeography
    return 2;
  }
}
