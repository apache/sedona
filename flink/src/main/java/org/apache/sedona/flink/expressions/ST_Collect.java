/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.Map;

/**
 * This class is basically a copy of
 * flink-table-runtime/src/main/java/org/apache/flink/table/runtime/functions/aggregate/CollectAggFunction.java/
 * specialized for Geometries, so it does return a GeometryCollection instead of Multiset of Geometries.
 * The type currently is always geometry collection even for homogenous geometries.
 */
@FunctionHint(
        input = @DataTypeHint(inputGroup = InputGroup.ANY, value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class),
        output = @DataTypeHint(inputGroup = InputGroup.ANY, value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class),
        accumulator = @DataTypeHint(bridgedTo = ST_Collect.GeometryCollectAccumulator.class)
)
public class ST_Collect extends AggregateFunction<Geometry, ST_Collect.GeometryCollectAccumulator> {
    private final transient DataType elementDataType;
    public ST_Collect(DataType geomType)
    {
        this.elementDataType = geomType;
    }

    /**
     *   Manual type inference because we need to specify
     *          - the output type (Raw bridged to geometry)
     *          - and for the Accumulator, because the key type of the mapview gets erased
     * @param typeFactory
     * @return
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {

        DataType accumulatorType = DataTypes.STRUCTURED(
                GeometryCollectAccumulator.class,
                DataTypes.FIELD(
                        "map",
                        MapView.newMapViewDataType(elementDataType.notNull(), DataTypes.INT())));

        return TypeInference.
                newBuilder().
                accumulatorTypeStrategy(new ExplicitTypeStrategy(accumulatorType)).
                outputTypeStrategy(new ExplicitTypeStrategy(elementDataType.bridgedTo(Geometry.class))).build();
    }

    public static class GeometryCollectAccumulator {
        @DataTypeHint(value = "MAP<RAW, INT>")
        public MapView<Geometry, Integer> map = new MapView<>();
    }


    @Override
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry getValue(ST_Collect.GeometryCollectAccumulator accumulator) {
        Map<Geometry, Integer> map = accumulator.map.getMap();
        GeometryFactory fact = new GeometryFactory();
        GeometryCollection collection = fact.createGeometryCollection(map.keySet().toArray(new Geometry[0]));
        return collection;
    }

    @Override
    public ST_Collect.GeometryCollectAccumulator createAccumulator() {
        final ST_Collect.GeometryCollectAccumulator acc = new ST_Collect.GeometryCollectAccumulator();
        return acc;
    }

    public void accumulate(ST_Collect.GeometryCollectAccumulator accumulator,
                          @DataTypeHint(inputGroup = InputGroup.ANY, value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) throws Exception {
        Geometry geom = (Geometry) o;
        if (geom != null) {
            Integer count = accumulator.map.get(geom);
            if (count != null) {
                accumulator.map.put(geom, count + 1);
            } else {
                accumulator.map.put(geom, 1);
            }
        }
    }

    public void retract(ST_Collect.GeometryCollectAccumulator accumulator,
                        @DataTypeHint(inputGroup =InputGroup.ANY, value ="RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) throws Exception {
        Geometry geom = (Geometry) o;
        if (geom != null) {
            Integer count = accumulator.map.get(geom);
            if (count != null) {
                if (count == 1) {
                    accumulator.map.remove(geom);
                } else {
                    accumulator.map.put(geom, count - 1);
                }
            } else {
                accumulator.map.put(geom, -1);
            }
        }
    }

    public void merge(ST_Collect.GeometryCollectAccumulator accumulator, Iterable<ST_Collect.GeometryCollectAccumulator> others)
            throws Exception {
        for (ST_Collect.GeometryCollectAccumulator other : others) {
            for (Map.Entry<Geometry, Integer> entry : other.map.entries()) {
                Geometry key = entry.getKey();
                Integer newCount = entry.getValue();
                Integer oldCount = accumulator.map.get(key);
                if (oldCount == null) {
                    accumulator.map.put(key, newCount);
                } else {
                    accumulator.map.put(key, oldCount + newCount);
                }
            }
        }
    }

    public void resetAccumulator(ST_Collect.GeometryCollectAccumulator accumulator) {
        accumulator.map.clear();
    }
}
