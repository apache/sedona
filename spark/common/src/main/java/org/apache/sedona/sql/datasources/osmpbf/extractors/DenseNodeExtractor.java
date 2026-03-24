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
package org.apache.sedona.sql.datasources.osmpbf.extractors;

import java.util.HashMap;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;

public class DenseNodeExtractor implements Extractor {
  long latOffset;
  long lonOffset;
  long granularity;
  int dateGranularity;
  long firstId;
  long firstLat;
  long firstLon;
  Integer keyIndex;

  // DenseInfo delta accumulators
  boolean hasDenseInfo;
  long firstTimestamp;
  long firstChangeset;
  int firstUid;
  int firstUserSid;

  Osmformat.DenseNodes nodes;

  public DenseNodeExtractor(
      Osmformat.DenseNodes nodes,
      long latOffset,
      long lonOffset,
      long granularity,
      int dateGranularity) {
    this.firstId = 0;
    this.firstLat = 0;
    this.firstLon = 0;
    this.latOffset = latOffset;
    this.lonOffset = lonOffset;
    this.granularity = granularity;
    this.dateGranularity = dateGranularity;
    this.nodes = nodes;
    this.keyIndex = 0;

    this.hasDenseInfo = nodes.hasDenseinfo() && nodes.getDenseinfo().getVersionCount() > 0;
    this.firstTimestamp = 0;
    this.firstChangeset = 0;
    this.firstUid = 0;
    this.firstUserSid = 0;
  }

  public OsmNode extract(int idx, Osmformat.StringTable stringTable) {
    return parse(idx, stringTable);
  }

  private OsmNode parse(int idx, Osmformat.StringTable stringTable) {
    long id = nodes.getId(idx) + firstId;
    long latitude = nodes.getLat(idx) + firstLat;
    long longitude = nodes.getLon(idx) + firstLon;

    double lat = .000000001 * (latOffset + (latitude * granularity));
    double lon = .000000001 * (lonOffset + (longitude * granularity));

    firstId = id;
    firstLat = latitude;
    firstLon = longitude;

    HashMap<String, String> tags = parseTags(stringTable);

    OsmNode node = new OsmNode(id, lat, lon, tags);

    if (hasDenseInfo) {
      Osmformat.DenseInfo denseInfo = nodes.getDenseinfo();

      // version is NOT delta-encoded
      if (denseInfo.getVersionCount() > idx) {
        node.setVersion(denseInfo.getVersion(idx));
      }

      // timestamp, changeset, uid, user_sid are delta-encoded
      if (denseInfo.getTimestampCount() > idx) {
        long timestamp = denseInfo.getTimestamp(idx) + firstTimestamp;
        firstTimestamp = timestamp;
        node.setTimestamp(timestamp * dateGranularity);
      }

      if (denseInfo.getChangesetCount() > idx) {
        long changeset = denseInfo.getChangeset(idx) + firstChangeset;
        firstChangeset = changeset;
        node.setChangeset(changeset);
      }

      if (denseInfo.getUidCount() > idx) {
        int uid = denseInfo.getUid(idx) + firstUid;
        firstUid = uid;
        node.setUid(uid);
      }

      if (denseInfo.getUserSidCount() > idx) {
        int userSid = denseInfo.getUserSid(idx) + firstUserSid;
        firstUserSid = userSid;
        if (userSid > 0) {
          node.setUser(stringTable.getS(userSid).toStringUtf8());
        }
      }

      // visible is NOT delta-encoded, and may not be present
      if (denseInfo.getVisibleCount() > idx) {
        node.setVisible(denseInfo.getVisible(idx));
      } else {
        // Per OSM PBF spec, missing 'visible' must be treated as true (especially in history files)
        node.setVisible(true);
      }
    }

    return node;
  }

  HashMap<String, String> parseTags(Osmformat.StringTable stringTable) {
    HashMap<String, String> tags = new HashMap<>();

    while (nodes.getKeysVals(keyIndex) != 0) {
      int key = nodes.getKeysVals(keyIndex);
      int value = nodes.getKeysVals(keyIndex + 1);

      String keyString = stringTable.getS(key).toStringUtf8();
      String valueString = stringTable.getS(value).toStringUtf8();

      tags.put(keyString, valueString);

      keyIndex = keyIndex + 2;
    }

    keyIndex = keyIndex + 1;

    return tags;
  }
}
