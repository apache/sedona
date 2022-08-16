/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.flink;

import org.apache.calcite.runtime.Geometries;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.GeometrySerde;
import org.apache.sedona.core.geometryObjects.SpatialIndexSerde;
import org.apache.sedona.core.spatialPartitioning.PartitioningUtils;
import org.apache.sedona.flink.expressions.ST_Collect;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.Arrays;

public class SedonaFlinkRegistrator {

    public static void registerFunc(StreamTableEnvironment tblEnv) {
        Arrays.stream(Catalog.getFuncs()).forEach(
                func -> tblEnv.createTemporarySystemFunction(func.getClass().getSimpleName(), func)
        );
        Arrays.stream(Catalog.getPredicates()).forEach(
                func -> tblEnv.createTemporarySystemFunction(func.getClass().getSimpleName(), func)
        );
    }

    /**
     * This is needed because for Resolving the RAW Geometry DataType in ST_Collect (MapView in Accumulator)
     * the serializer needs to be known. To get the registered Serializer the ExecutionEnvironment is needed and
     * to add the UDF the TableEnvironment ist needed.
     * @param env
     * @param tblEnv
     */
    public static void registerCollect(StreamExecutionEnvironment env, StreamTableEnvironment tblEnv)
    {
        KryoSerializer<Geometry> serializer = new KryoSerializer(Geometry.class, env.getConfig());
        UserDefinedFunction fun = new ST_Collect(DataTypes.RAW(Geometry.class, serializer));
        tblEnv.createTemporarySystemFunction(fun.getClass().getSimpleName(), fun);
    }

    public static void registerType(StreamExecutionEnvironment env) {
        GeometrySerde serializer = new GeometrySerde();
        SpatialIndexSerde indexSerializer = new SpatialIndexSerde(serializer);
        env.getConfig().registerTypeWithKryoSerializer(Geometry.class, serializer);
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
    }
}
