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

/** Generates geometries that are distributed according to the Bit distribution */
public class BitGenerator extends PointBasedGenerator {

  public static class BitParameter {
    public final PointBasedParameter pointBasedParameter;

    /** The probability of setting a bit */
    public final double probability;

    /** Number of digits in the generated data */
    public final int digits;

    public BitParameter(PointBasedParameter pointBasedParameter, double probability, int digits) {
      if (probability < 0 || probability > 1) {
        throw new IllegalArgumentException("Probability must be between 0 and 1");
      }
      if (digits <= 0) {
        throw new IllegalArgumentException("Digits must be a positive integer");
      }
      this.pointBasedParameter = pointBasedParameter;
      this.probability = probability;
      this.digits = digits;
    }

    public static BitParameter create(Map<String, String> conf) {
      PointBasedParameter pointBasedParameter = PointBasedParameter.create(conf);
      double probability = Double.parseDouble(conf.getOrDefault("probability", "0.2"));
      int digits = Integer.parseInt(conf.getOrDefault("digits", "10"));
      return new BitParameter(pointBasedParameter, probability, digits);
    }
  }

  private final BitParameter parameter;

  public BitGenerator(Random random, BitParameter bitParameter) {
    super(random, bitParameter.pointBasedParameter);
    this.parameter = bitParameter;
  }

  @Override
  protected Coordinate generateCoordinate() {
    double x = generateCoordinateValue();
    double y = generateCoordinateValue();
    return new Coordinate(x, y);
  }

  private double generateCoordinateValue() {
    double n = 0.0;
    for (int i = 1; i <= parameter.digits; i++) {
      double bit = bernoulli(parameter.probability);
      n += bit / (1 << i);
    }
    return n;
  }
}
