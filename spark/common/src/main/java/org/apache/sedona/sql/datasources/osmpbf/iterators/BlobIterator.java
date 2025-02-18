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

import static org.apache.sedona.sql.datasources.osmpbf.ParseUtils.dataInputStreamBlob;

import java.io.IOException;
import java.util.Iterator;
import java.util.zip.DataFormatException;
import org.apache.sedona.sql.datasources.osmpbf.DenseNodeIterator;
import org.apache.sedona.sql.datasources.osmpbf.build.Fileformat.Blob;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.extractors.DenseNodeExtractor;
import org.apache.sedona.sql.datasources.osmpbf.extractors.RelationExtractor;
import org.apache.sedona.sql.datasources.osmpbf.extractors.WaysExtractor;
import org.apache.sedona.sql.datasources.osmpbf.model.OSMEntity;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;

public class BlobIterator implements Iterator<OSMEntity> {
  Blob blob;
  Osmformat.PrimitiveBlock primitiveBlock;
  int primitiveGroupIdx;
  int osmEntityIdx;

  DenseNodeIterator denseNodesIterator;

  Osmformat.PrimitiveGroup currentPrimitiveGroup;

  public BlobIterator(Blob blob) throws DataFormatException, IOException {
    primitiveBlock = Osmformat.PrimitiveBlock.parseFrom(dataInputStreamBlob(blob));
    primitiveGroupIdx = 0;
    osmEntityIdx = 0;

    currentPrimitiveGroup = primitiveBlock.getPrimitivegroup(primitiveGroupIdx);

    this.blob = blob;
  }

  @Override
  public boolean hasNext() {
    return primitiveBlock.getPrimitivegroupList().size() != primitiveGroupIdx;
  }

  @Override
  public OSMEntity next() {
    if (currentPrimitiveGroup == null) {
      return null;
    }

    if (!currentPrimitiveGroup.getRelationsList().isEmpty()) {
      return extractRelationPrimitiveGroup();
    }

    if (!currentPrimitiveGroup.getNodesList().isEmpty()) {
      return null;
    }

    if (!currentPrimitiveGroup.getWaysList().isEmpty()) {
      return extractWayPrimitiveGroup();
    }

    if (!currentPrimitiveGroup.getChangesetsList().isEmpty()) {
      return null;
    }

    if (currentPrimitiveGroup.getDense() != null) {
      return extractDenseNodePrimitiveGroup();
    }

    return null;
  }

  public OSMEntity extractDenseNodePrimitiveGroup() {
    if (denseNodesIterator == null) {
      denseNodesIterator =
          new DenseNodeIterator(
              currentPrimitiveGroup.getDense().getIdCount(),
              primitiveBlock.getStringtable(),
              new DenseNodeExtractor(
                  currentPrimitiveGroup.getDense(),
                  primitiveBlock.getLatOffset(),
                  primitiveBlock.getLonOffset(),
                  primitiveBlock.getGranularity()));
    }

    OsmNode node = denseNodesIterator.next();

    if (!denseNodesIterator.hasNext()) {
      denseNodesIterator = null;
      nextEntity();
    }

    return node;
  }

  public OSMEntity extractWayPrimitiveGroup() {
    osmEntityIdx += 1;
    if (currentPrimitiveGroup.getWaysList().size() == osmEntityIdx) {
      nextEntity();
    }

    return new WaysExtractor(currentPrimitiveGroup, primitiveBlock.getStringtable())
        .extract(osmEntityIdx);
  }

  public OSMEntity extractRelationPrimitiveGroup() {
    osmEntityIdx += 1;
    if (currentPrimitiveGroup.getRelationsList().size() == osmEntityIdx) {
      nextEntity();
    }

    Osmformat.StringTable stringTable = primitiveBlock.getStringtable();

    return new RelationExtractor(currentPrimitiveGroup, stringTable).extract(osmEntityIdx);
  }

  public void nextEntity() {
    primitiveGroupIdx += 1;
    osmEntityIdx = 0;
  }
}
