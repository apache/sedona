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
    GeometrySerde serializer = new GeometrySerde();
    SpatialIndexSerde indexSerializer = new SpatialIndexSerde(serializer);
    env.getConfig().registerTypeWithKryoSerializer(Point.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(LineString.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(Polygon.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(MultiPoint.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(MultiLineString.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(MultiPolygon.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(GeometryCollection.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(Circle.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(Envelope.class, serializer);
    env.getConfig().registerTypeWithKryoSerializer(Quadtree.class, indexSerializer);
    env.getConfig().registerTypeWithKryoSerializer(STRtree.class, indexSerializer);

    Arrays.stream(Catalog.getFuncs())
        .forEach(
            func -> tblEnv.createTemporarySystemFunction(func.getClass().getSimpleName(), func));
    Arrays.stream(Catalog.getPredicates())
        .forEach(
            func -> tblEnv.createTemporarySystemFunction(func.getClass().getSimpleName(), func));
    return tblEnv;
  }
}
