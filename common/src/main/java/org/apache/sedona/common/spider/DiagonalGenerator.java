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
package org.apache.sedona.common.spider;

import java.util.Map;
import java.util.Random;
import org.locationtech.jts.geom.Coordinate;

/** Generates geometries that are distributed according to the Diagonal distribution */
public class DiagonalGenerator extends PointBasedGenerator {

  public static class DiagonalParameter {
    public final PointBasedParameter pointBasedParameter;

    /** The percentage of records that are perfectly on the diagonal */
    public final double percentage;

    /** For points not exactly on the diagonal, the buffer in which they are dispersed */
    public final double buffer;

    public DiagonalParameter(
        PointBasedParameter pointBasedParameter, double percentage, double buffer) {
      this.pointBasedParameter = pointBasedParameter;
      this.percentage = percentage;
      this.buffer = buffer;
    }

    public static DiagonalParameter create(Map<String, String> conf) {
      PointBasedParameter pointBasedParameter = PointBasedParameter.create(conf);
      double percentage = Double.parseDouble(conf.getOrDefault("percentage", "0.5"));
      double buffer = Double.parseDouble(conf.getOrDefault("buffer", "0.5"));
      if (percentage < 0 || percentage > 1) {
        throw new IllegalArgumentException("Percentage must be between 0 and 1");
      }
      if (buffer < 0) {
        throw new IllegalArgumentException("Buffer must be a non-negative number");
      }
      return new DiagonalParameter(pointBasedParameter, percentage, buffer);
    }
  }

  private static final double INV_SQRT2 = 1 / Math.sqrt(2);

  private final DiagonalParameter parameter;

  public DiagonalGenerator(Random random, DiagonalParameter diagonalParameter) {
    super(random, diagonalParameter.pointBasedParameter);
    this.parameter = diagonalParameter;
  }

  @Override
  protected Coordinate generateCoordinate() {
    if (bernoulli(parameter.percentage) == 1) {
      double position = uniform(0, 1);
      return new Coordinate(position, position);
    } else {
      double c = uniform(0, 1);
      double d = normal(0, parameter.buffer / 5);
      double displacement = d * INV_SQRT2;
      double x = c + displacement;
      double y = c - displacement;
      return new Coordinate(x, y);
    }
  }
}
