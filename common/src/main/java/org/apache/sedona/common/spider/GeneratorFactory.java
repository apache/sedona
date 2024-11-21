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
import org.apache.sedona.common.spider.PointBasedGenerator.PointBasedParameter;

/** A factory class for creating instances of {@link Generator} */
public class GeneratorFactory {
  private GeneratorFactory() {}

  /**
   * Creates an instance of {@link Generator} with the given name, random number generator, and
   * configuration.
   *
   * @param name the name of the generator
   * @param random the random number generator
   * @param parameters the configuration
   * @return an instance of {@link Generator}
   */
  public static Generator create(String name, Random random, Map<String, String> parameters) {
    switch (name) {
      case "uniform":
        return new UniformGenerator(random, PointBasedParameter.create(parameters));
      case "gaussian":
        return new GaussianGenerator(random, PointBasedParameter.create(parameters));
      case "diagonal":
        return new DiagonalGenerator(
            random, DiagonalGenerator.DiagonalParameter.create(parameters));
      case "bit":
        return new BitGenerator(random, BitGenerator.BitParameter.create(parameters));
      case "sierpinski":
        return new SierpinskiGenerator(random, PointBasedParameter.create(parameters));
      case "parcel":
        return new ParcelGenerator(random, ParcelGenerator.ParcelParameter.create(parameters));
      default:
        throw new IllegalArgumentException("Unknown generator: " + name);
    }
  }
}
