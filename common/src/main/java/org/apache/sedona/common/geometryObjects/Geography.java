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
  // Hold the underlying S2Geography directly, not another Geography
  private final S2Geography delegate;

  public Geography(S2Geography delegate) {
    // Initialize super with the correct kind
    super(GeographyKind.fromKind(delegate.getKind()));
    this.delegate = delegate;
  }

  /** Return the wrapped S2Geography. */
  public Geography getGeography() {
    return (Geography) delegate;
  }

  public S2Geography getDelegate() {
    return delegate;
  }

  @Override
  public int dimension() {
    return delegate.dimension();
  }

  @Override
  public int numShapes() {
    return delegate.numShapes();
  }

  @Override
  public S2Shape shape(int id) {
    return delegate.shape(id);
  }

  @Override
  public S2Region region() {
    return delegate.region();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  protected void encode(UnsafeOutput os, EncodeOptions opts) throws IOException {
    // Delegate encoding to the wrapped object
    delegate.encodeTagged(os, opts);
  }
}
