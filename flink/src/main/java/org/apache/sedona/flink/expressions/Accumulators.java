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
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.locationtech.jts.geom.Geometry;

/** Mutable accumulator of structured type for the aggregate function */
public class Accumulators {

  public static class Envelope {
    public double minX = Double.MAX_VALUE;
    public double minY = Double.MAX_VALUE;
    public double maxX = -Double.MAX_VALUE;
    public double maxY = -Double.MAX_VALUE;

    void reset() {
      minX = Double.MAX_VALUE;
      minY = Double.MAX_VALUE;
      maxX = -Double.MAX_VALUE;
      maxY = -Double.MAX_VALUE;
    }
  }

  public static class AccGeometry {
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry geom;
  }

  public static class AccGeometry2 {
    public Geometry geom1;
    public Geometry geom2;
  }

  public static class AccGeometryN {
    public Geometry[] geoms;
    public int numGeoms;

    AccGeometryN(int numGeoms) {
      this.geoms = new Geometry[numGeoms];
      this.numGeoms = numGeoms;
    }
  }
}
