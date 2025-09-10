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
import org.apache.sedona.sql.datasources.osmpbf.features.TagsResolver;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;

public class NodeExtractor {

  Osmformat.PrimitiveGroup primitiveGroup;
  Osmformat.PrimitiveBlock primitiveBlock;

  public NodeExtractor(
      Osmformat.PrimitiveGroup primitiveGroup, Osmformat.PrimitiveBlock primitiveBlock) {
    this.primitiveGroup = primitiveGroup;
    this.primitiveBlock = primitiveBlock;
  }

  public OsmNode extract(int idx, Osmformat.StringTable stringTable) {
    return parse(idx, stringTable);
  }

  private OsmNode parse(int idx, Osmformat.StringTable stringTable) {
    Osmformat.Node node = primitiveGroup.getNodes(idx);

    long id = node.getId();
    long latitude = node.getLat();
    long longitude = node.getLon();

    long latOffset = primitiveBlock.getLatOffset();
    long lonOffset = primitiveBlock.getLonOffset();
    long granularity = primitiveBlock.getGranularity();

    // https://wiki.openstreetmap.org/wiki/PBF_Format
    // latitude = .000000001 * (lat_offset + (granularity * lat))
    // longitude = .000000001 * (lon_offset + (granularity * lon))
    double lat = .000000001 * (latOffset + (latitude * granularity));
    double lon = .000000001 * (lonOffset + (longitude * granularity));

    HashMap<String, String> tags =
        TagsResolver.resolveTags(node.getKeysCount(), node::getKeys, node::getVals, stringTable);

    return new OsmNode(id, lat, lon, tags);
  }
}
