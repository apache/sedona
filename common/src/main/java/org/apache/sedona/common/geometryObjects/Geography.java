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

import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2Shape;
import java.io.IOException;
import org.apache.sedona.common.S2Geography.EncodeOptions;
import org.apache.sedona.common.S2Geography.S2Geography;

public class Geography extends S2Geography {
  private final Geography geography;

  public Geography(S2Geography geography) {
    super(GeographyKind.fromKind(geography.getKind()));
    this.geography = (Geography) geography;
  }

  public Geography getGeographhy() {
    return this.geography;
  }

  @Override
  public int dimension() {
    return this.geography.dimension();
  }

  @Override
  public int numShapes() {
    return this.geography.numShapes();
  }

  @Override
  public S2Shape shape(int id) {
    return this.geography.shape(id);
  }

  @Override
  public S2Region region() {
    return this.geography.region();
  }

  public String toString() {
    return this.geography.toString();
  }

  @Override
  protected void encode(UnsafeOutput os, EncodeOptions opts) throws IOException {}
}
