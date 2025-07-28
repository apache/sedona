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
package org.apache.sedona.common.geometryObjects;

import org.apache.sedona.common.S2Geography.S2Geography;
import org.locationtech.jts.geom.PrecisionModel;

public abstract class S2GeographyWrapper extends S2Geography {
  private final S2GeographyWrapper geography;

  public S2GeographyWrapper(S2GeographyWrapper geography) {
    super(GeographyKind.fromKind(geography.getKind()));
    this.geography = geography;
  }

  public S2GeographyWrapper getGeography() {
    return this.geography;
  }

  @Override
  public String toString() {
    return geography.toText(new PrecisionModel());
  }
}
