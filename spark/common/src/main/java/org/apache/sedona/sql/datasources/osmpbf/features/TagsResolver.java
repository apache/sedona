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
package org.apache.sedona.sql.datasources.osmpbf.features;

import java.util.HashMap;
import java.util.function.Function;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;

public class TagsResolver {
  public static HashMap<String, String> resolveTags(
      int keysCount,
      Function<Integer, Integer> getKey,
      Function<Integer, Integer> getValue,
      Osmformat.StringTable stringTable) {
    HashMap<String, String> tags = new HashMap<>();

    for (int i = 0; i < keysCount; i++) {
      int key = getKey.apply(i);
      int value = getValue.apply(i);

      String keyString = stringTable.getS(key).toStringUtf8();
      String valueString = stringTable.getS(value).toStringUtf8();
      tags.put(keyString, valueString);
    }

    return tags;
  }
}
