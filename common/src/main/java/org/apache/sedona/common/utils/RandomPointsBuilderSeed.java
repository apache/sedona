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

import java.util.Random;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.shape.random.RandomPointsBuilder;

public class RandomPointsBuilderSeed extends RandomPointsBuilder {
  Random rand;

  public RandomPointsBuilderSeed(GeometryFactory geometryFactory, long seed) {
    super(geometryFactory);
    if (seed > 0) {
      this.rand = new Random(seed);
    } else {
      this.rand = new Random();
    }
  }

  public RandomPointsBuilderSeed(GeometryFactory geometryFactory, Random random) {
    super(geometryFactory);
    this.rand = random;
  }

  @Override
  protected Coordinate createRandomCoord(Envelope env) {
    double x = env.getMinX() + env.getWidth() * rand.nextDouble();
    double y = env.getMinY() + env.getHeight() * rand.nextDouble();
    return createCoord(x, y);
  }
}
