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

import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OSMEntity;

public class InfoResolver {

  public static void populateInfo(
      OSMEntity entity,
      Osmformat.Info info,
      Osmformat.StringTable stringTable,
      int dateGranularity) {
    if (info == null) {
      return;
    }
    entity.setVersion(info.getVersion());
    entity.setTimestamp((long) info.getTimestamp() * dateGranularity);
    entity.setChangeset(info.getChangeset());
    entity.setUid(info.getUid());
    if (info.getUserSid() > 0) {
      entity.setUser(stringTable.getS(info.getUserSid()).toStringUtf8());
    }
    if (info.hasVisible()) {
      entity.setVisible(info.getVisible());
    }
  }
}
