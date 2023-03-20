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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class Utils
{
    static Long timestamp_base = new Timestamp(System.currentTimeMillis()).getTime();
    static Long time_interval = 1L; // Generate a record per this interval. Unit is second

    static List<Row> createPointText(int size){
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create a number of points (1, 1) (2, 2) ...
            data.add(Row.of(i + "," + i, "point" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPolygonText(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
            String minX = String.valueOf(i - 0.5);
            String minY = String.valueOf(i - 0.5);
            String maxX = String.valueOf(i + 0.5);
            String maxY = String.valueOf(i + 0.5);
            List<String> polygon = new ArrayList<>();
            polygon.add(minX);polygon.add(minY);
            polygon.add(minX);polygon.add(maxY);
            polygon.add(maxX);polygon.add(maxY);
            polygon.add(maxX);polygon.add(minY);
            polygon.add(minX);polygon.add(minY);
            data.add(Row.of(String.join(",", polygon), "polygon" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPointWKT(int size){
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create a number of points (1, 1) (2, 2) ...
            data.add(Row.of("POINT (" + i + " " + i +")", "point" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPolygonWKT(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
            String minX = String.valueOf(i - 0.5);
            String minY = String.valueOf(i - 0.5);
            String maxX = String.valueOf(i + 0.5);
            String maxY = String.valueOf(i + 0.5);
            List<String> polygon = new ArrayList<>();
            polygon.add(minX + " " + minY);
            polygon.add(minX + " " + maxY);
            polygon.add(maxX + " " + maxY);
            polygon.add(maxX + " " + minY);
            polygon.add(minX + " " + minY);
            data.add(Row.of("POLYGON ((" + String.join(", ", polygon) + "))", "polygon" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static Table createTextTable(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, List<Row> data, String[] colNames){
        TypeInformation<?>[] colTypes = {
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };
        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(colNames, 0, 3));
        DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
        // Generate Time Attribute
        WatermarkStrategy<Row> wmStrategy =
                WatermarkStrategy
                        .<Row>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs(2));
        return tableEnv.fromDataStream(ds.assignTimestampsAndWatermarks(wmStrategy), $(colNames[0]), $(colNames[1]), $(colNames[2]).rowtime(), $(colNames[3]).proctime());
    }


}
