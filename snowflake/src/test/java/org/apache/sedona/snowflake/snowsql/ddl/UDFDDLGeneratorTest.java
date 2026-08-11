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
package org.apache.sedona.snowflake.snowsql.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class UDFDDLGeneratorTest {

  @Test
  public void geometryOnlyFunctionsAreNotGeneratedForGeography() {
    Constants.snowflakeTypeMap.replace("Geometry", "GEOMETRY");
    Map<String, String> configs = new HashMap<>();
    configs.put(Constants.SEDONA_VERSION, "test");
    configs.put(Constants.GEOTOOLS_VERSION, "test");

    try {
      List<String> ddls = UDFDDLGenerator.buildAll(configs, "@ApacheSedona", false, "");
      List<String> sharedPathsDdls =
          ddls.stream()
              .filter(ddl -> ddl.contains(".ST_SharedPaths "))
              .collect(Collectors.toList());

      assertEquals(2, sharedPathsDdls.size());
      assertFalse(sharedPathsDdls.stream().anyMatch(ddl -> ddl.contains(" GEOGRAPHY")));
    } finally {
      Constants.snowflakeTypeMap.replace("Geometry", "GEOMETRY");
    }
  }
}
