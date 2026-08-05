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
package org.apache.sedona.flink;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.geometrySerde.GeometrySerde;
import org.apache.sedona.common.geometrySerde.SpatialIndexSerde;
import org.apache.sedona.common.utils.TelemetryCollector;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

public class SedonaContext {
  /**
   * This is the entry point of the entire Sedona system
   *
   * @param env
   * @param tblEnv
   * @return
   */
  public static StreamTableEnvironment create(
      StreamExecutionEnvironment env, StreamTableEnvironment tblEnv) {
    TelemetryCollector.send("flink", "java");
    registerGeometryKryoSerializers(env);

    Arrays.stream(Catalog.getFuncs())
        .forEach(
            func -> tblEnv.createTemporarySystemFunction(func.getClass().getSimpleName(), func));
    Arrays.stream(Catalog.getPredicates())
        .forEach(
            func -> tblEnv.createTemporarySystemFunction(func.getClass().getSimpleName(), func));
    return tblEnv;
  }

  /**
   * ExecutionConfig.registerTypeWithKryoSerializer was removed; Flink 1.19+'s public replacement is
   * the declarative pipeline.serialization-config option (FLIP-398). Applied via
   * StreamExecutionEnvironment.configure(ReadableConfig, ClassLoader) —
   * env.getConfig().getSerializerConfig() is itself @Internal in both Flink 1.19 and 2.2, so this
   * goes through the environment's public API instead. The two-argument overload is used to pass
   * the calling thread's context classloader explicitly: the one-argument overload resolves classes
   * with the environment's own user classloader, which does not see PyFlink's job classloader and
   * fails PyFlink jobs with ClassNotFoundException. Flink instantiates
   * GeometrySerde/SpatialIndexSerde itself via their no-arg constructors, so only the class names
   * are registered here.
   */
  static void registerGeometryKryoSerializers(StreamExecutionEnvironment env) {
    List<String> kryoRegistrations =
        Arrays.asList(
            kryoRegistration(Point.class, GeometrySerde.class),
            kryoRegistration(LineString.class, GeometrySerde.class),
            kryoRegistration(Polygon.class, GeometrySerde.class),
            kryoRegistration(MultiPoint.class, GeometrySerde.class),
            kryoRegistration(MultiLineString.class, GeometrySerde.class),
            kryoRegistration(MultiPolygon.class, GeometrySerde.class),
            kryoRegistration(GeometryCollection.class, GeometrySerde.class),
            kryoRegistration(Circle.class, GeometrySerde.class),
            kryoRegistration(Envelope.class, GeometrySerde.class),
            kryoRegistration(Quadtree.class, SpatialIndexSerde.class),
            kryoRegistration(STRtree.class, SpatialIndexSerde.class));

    Configuration configuration = new Configuration();
    configuration.set(PipelineOptions.SERIALIZATION_CONFIG, kryoRegistrations);
    env.configure(configuration, Thread.currentThread().getContextClassLoader());
  }

  private static String kryoRegistration(Class<?> type, Class<?> serializer) {
    return type.getName()
        + ": {type: kryo, kryo-type: registered, class: "
        + serializer.getName()
        + "}";
  }
}
