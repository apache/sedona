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
package org.apache.sedona.sql.datasources.osmpbf.iterators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.features.TagsResolver;
import org.apache.sedona.sql.datasources.osmpbf.model.OSMEntity;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;

public class NodeIterator implements Iterator<OSMEntity> {
  int idx;
  long nodesCount;
  List<Osmformat.Node> nodes;
  Osmformat.StringTable stringTable;
  Osmformat.PrimitiveBlock primitiveBlock;

  public NodeIterator(List<Osmformat.Node> nodes, Osmformat.PrimitiveBlock primitiveBlock) {
    this.idx = 0;
    this.nodesCount = 0;
    this.nodes = nodes;
    this.stringTable = primitiveBlock.getStringtable();
    this.primitiveBlock = primitiveBlock;

    if (nodes != null) {
      this.nodesCount = nodes.size();
    }
  }

  @Override
  public boolean hasNext() {
    return idx < nodesCount;
  }

  @Override
  public OSMEntity next() {
    if (idx < nodesCount) {
      OsmNode node = extract(idx);
      idx++;
      return node;
    }

    return null;
  }

  public OsmNode extract(int idx) {
    return parse(idx);
  }

  private OsmNode parse(int idx) {
    Osmformat.Node node = nodes.get(idx);

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
