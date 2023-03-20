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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.sedona.flink.SedonaFlinkRegistrator;
import org.apache.sedona.flink.expressions.Constructors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkExample
{
    static String[] pointColNames = {"geom_point", "name_point", "event_time", "proc_time"};

    static String[] polygonColNames = {"geom_polygon", "name_polygon", "event_time", "proc_time"};

    public static void main(String[] args) {
        int testDataSize = 10;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);

        // Create a fake WKT string table source
        Table pointWktTable = Utils.createTextTable(env, tableEnv, Utils.createPointWKT(testDataSize), pointColNames);
        // Create a geometry column
        Table pointTable = pointWktTable.select(call(Constructors.ST_GeomFromWKT.class.getSimpleName(),
                        $(pointColNames[0])).as(pointColNames[0]),
                $(pointColNames[1]));
        // Create S2CellID
        pointTable = pointTable.select($(pointColNames[0]), $(pointColNames[1]),
                call("ST_S2CellIDs", $(pointColNames[0]), 6).as("s2id_array"));
        // Explode s2id array
        tableEnv.createTemporaryView("pointTable", pointTable);
        pointTable = tableEnv.sqlQuery("SELECT geom_point, name_point, s2id_point FROM pointTable CROSS JOIN UNNEST(pointTable.s2id_array) AS tmpTbl1(s2id_point)");
        pointTable.execute().print();


        // Create a fake WKT string table source
        Table polygonWktTable = Utils.createTextTable(env, tableEnv, Utils.createPolygonWKT(testDataSize), polygonColNames);
        // Create a geometry column
        Table polygonTable = polygonWktTable.select(call(Constructors.ST_GeomFromWKT.class.getSimpleName(),
                        $(polygonColNames[0])).as(polygonColNames[0]),
                $(polygonColNames[1]));
        // Create S2CellID
        polygonTable = polygonTable.select($(polygonColNames[0]), $(polygonColNames[1]),
                call("ST_S2CellIDs", $(polygonColNames[0]), 6).as("s2id_array"));
        // Explode s2id array
        tableEnv.createTemporaryView("polygonTable", polygonTable);
        polygonTable = tableEnv.sqlQuery("SELECT geom_polygon, name_polygon, s2id_polygon FROM polygonTable CROSS JOIN UNNEST(polygonTable.s2id_array) AS tmpTbl2(s2id_polygon)");
        polygonTable.execute().print();

        // Join two tables by their S2 ids
        Table joinResult = pointTable.join(polygonTable).where($("s2id_point").isEqual($("s2id_polygon")));
        // Optional: remove false positives
        joinResult = joinResult.where("ST_Contains(geom_polygon, geom_point)");
        joinResult.execute().print();
    }

}
