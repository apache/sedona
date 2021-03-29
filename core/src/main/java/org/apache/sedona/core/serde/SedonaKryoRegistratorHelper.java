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

package org.apache.sedona.core.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import org.apache.log4j.Logger;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.GeometrySerde;
import org.apache.sedona.core.geometryObjects.WKBGeometrySerde;
import org.apache.sedona.core.serde.spatialindex.SpatialIndexSerde;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * Register Kryo classes by the type of geometry serde mechanism
 */
public class SedonaKryoRegistratorHelper {

    final static Logger log = Logger.getLogger(SedonaWKBKryoRegistrator.class);

    public static void registerClasses(Kryo kryo, Serializer geometrySerdeType, SpatialIndexSerde indexSerializer) {
        log.info("Registering custom serializers for geometry types");

        Serializer serializer;
        if (geometrySerdeType instanceof WKBGeometrySerde) {
            serializer = new WKBGeometrySerde();
        } else if (geometrySerdeType instanceof GeometrySerde) {
            serializer = new GeometrySerde();
        } else
            throw new UnsupportedOperationException(String.format("Geometry Serde: %s is not supported",
                    geometrySerdeType.getClass().getName())
            );

        kryo.register(Point.class, serializer);
        kryo.register(LineString.class, serializer);
        kryo.register(Polygon.class, serializer);
        kryo.register(MultiPoint.class, serializer);
        kryo.register(MultiLineString.class, serializer);
        kryo.register(MultiPolygon.class, serializer);
        kryo.register(GeometryCollection.class, serializer);
        kryo.register(Circle.class, serializer);
        kryo.register(Envelope.class, serializer);

        kryo.register(Quadtree.class, indexSerializer);
        kryo.register(STRtree.class, indexSerializer);
    }

}
