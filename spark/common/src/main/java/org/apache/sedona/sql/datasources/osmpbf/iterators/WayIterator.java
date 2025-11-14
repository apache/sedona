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
import org.apache.sedona.sql.datasources.osmpbf.model.Way;

public class WayIterator implements Iterator<OSMEntity> {
  int idx;
  long waysCount;
  List<Osmformat.Way> ways;
  Osmformat.StringTable stringTable;

  public WayIterator(List<Osmformat.Way> ways, Osmformat.StringTable stringTable) {
    this.idx = 0;
    this.waysCount = 0;
    this.ways = ways;
    this.stringTable = stringTable;

    if (ways != null) {
      this.waysCount = ways.size();
    }
  }

  @Override
  public boolean hasNext() {
    return idx < waysCount;
  }

  @Override
  public Way next() {
    if (idx < waysCount) {
      Way way = extract(idx);
      idx++;
      return way;
    }

    return null;
  }

  private Way extract(int idx) {
    Osmformat.Way way = ways.get(idx);

    return parse(way);
  }

  private Way parse(Osmformat.Way way) {
    long[] refs = new long[way.getRefsCount()];

    long firstRef = 0;
    if (way.getRefsCount() != 0) {
      for (int i = 0; i < way.getRefsCount(); i++) {
        firstRef += way.getRefs(i);
        refs[i] = firstRef;
      }
    }

    HashMap<String, String> tags =
        TagsResolver.resolveTags(way.getKeysCount(), way::getKeys, way::getVals, stringTable);

    return new Way(way.getId(), tags, refs);
  }
}
