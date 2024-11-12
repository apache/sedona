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

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * A generator is an iterator that generates random geometries. The actual implementation of the
 * generator is defined in the subclasses. You can create an instance of the generator by calling
 * the factory method {@link GeneratorFactory#create(String, Random, Map)} with the generator name,
 * a random number generator, and the configuration.
 *
 * <p>The idea and algorithms of this generator comes from this publication:
 *
 * <pre>
 * Puloma Katiyar, Tin Vu, Sara Migliorini, Alberto Belussi, Ahmed Eldawy.
 * "SpiderWeb: A Spatial Data Generator on the Web", ACM SIGSPATIAL 2020, Seattle, WA
 * </pre>
 */
public interface Generator extends Iterator<Geometry> {
  GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
}
