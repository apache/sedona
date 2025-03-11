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
package org.apache.sedona.sql.datasources.osmpbf;

import java.util.Iterator;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.extractors.DenseNodeExtractor;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;

public class DenseNodeIterator implements Iterator<OsmNode> {
  Osmformat.StringTable stringTable;
  int idx;
  long nodesSize;
  DenseNodeExtractor extractor;

  public DenseNodeIterator(
      long nodesSize, Osmformat.StringTable stringTable, DenseNodeExtractor extractor) {
    this.stringTable = stringTable;
    this.nodesSize = nodesSize;
    this.idx = 0;
    this.extractor = extractor;
  }

  @Override
  public boolean hasNext() {
    return idx < nodesSize;
  }

  @Override
  public OsmNode next() {
    OsmNode node = extractor.extract(idx, stringTable);
    idx += 1;
    return node;
  }
}
