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

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Constructors;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdapterTest extends TestBase {
  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  @Test
  public void testTableToDS() throws Exception {
    List<Row> data = createPolygonWKT(testDataSize);
    Table wktTable = createTextTable(data, polygonColNames);
    Table geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromWKT.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    Row result = last(geomTable);
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    // GeomTable to GeomDS
    DataStream<Row> geomStream = tableEnv.toDataStream(geomTable);
    assertEquals(
        data.get(0).getField(0).toString(),
        geomStream.executeAndCollect(1).get(0).getField(0).toString());
    // GeomDS to GeomTable
    geomTable = tableEnv.fromDataStream(geomStream);
    result = last(geomTable);
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
  }
}
