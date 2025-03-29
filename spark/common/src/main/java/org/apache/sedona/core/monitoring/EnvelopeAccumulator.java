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
package org.apache.sedona.core.monitoring;

import org.apache.spark.util.AccumulatorV2;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * An accumulator that accumulates the envelope of geometries.
 *
 * <p>Useful to determine the spatial extent of data in the task and stage.
 */
public class EnvelopeAccumulator extends AccumulatorV2<Envelope, Geometry> {
  private final GeometryFactory gf = new GeometryFactory();
  private Envelope env = new Envelope();

  @Override
  public boolean isZero() {
    return env.isNull();
  }

  @Override
  public AccumulatorV2<Envelope, Geometry> copy() {
    EnvelopeAccumulator newAcc = new EnvelopeAccumulator();
    newAcc.env = new Envelope(this.env);
    return newAcc;
  }

  @Override
  public void reset() {
    env = new Envelope();
  }

  @Override
  public void add(Envelope v) {
    env.expandToInclude(v);
  }

  @Override
  public void merge(AccumulatorV2<Envelope, Geometry> other) {
    env.expandToInclude(other.value().getEnvelopeInternal());
  }

  @Override
  public Geometry value() {
    return gf.toGeometry(env);
  }
}
