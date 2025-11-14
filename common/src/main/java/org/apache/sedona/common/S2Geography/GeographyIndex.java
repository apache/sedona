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
package org.apache.sedona.common.S2Geography;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Iterator;
import com.google.common.geometry.S2ShapeIndex;
import java.util.*;

public class GeographyIndex {
  private final S2ShapeIndex index;
  private final List<Integer> values;

  public GeographyIndex() {
    this(new S2ShapeIndex.Options());
  }

  public GeographyIndex(S2ShapeIndex.Options options) {
    this.index = new S2ShapeIndex(options);
    this.values = new ArrayList<>(Collections.singletonList(-1)); // list of shape id
  }

  public void add(Geography geog, int value) {
    for (int i = 0; i < geog.numShapes(); i++) {
      index.add(geog.shape(i));
      int shapeId = index.getShapes().size();
      values.add(shapeId, value);
    }
  }
  /** Returns the stored value for a given shape ID. */
  public int value(int shapeId) {
    return values.get(shapeId);
  }

  /** Provides read-only access to the underlying S2ShapeIndex. */
  public S2ShapeIndex getShapeIndex() {
    return index;
  }

  /** Iterator to query indexed shapes by S2CellId coverings. */
  public static class Iterator {
    private final GeographyIndex parent;
    private final S2Iterator.ListIterator<S2ShapeIndex.Cell> iterator;

    public Iterator(GeographyIndex index) {
      this.parent = index;
      this.iterator = index.getShapeIndex().iterator();
    }

    /** Query all cell IDs in the covering list, collecting associated values. */
    public void query(Iterable<S2CellId> covering, Set<Integer> result) {
      for (S2CellId cell : covering) {
        query(cell, result);
      }
    }

    /** Query a single cell ID, adding any intersecting values to result. */
    public void query(S2CellId cellId, Set<Integer> result) {
      S2ShapeIndex.CellRelation relation = iterator.locate(cellId);
      if (relation == S2ShapeIndex.CellRelation.INDEXED) {
        S2ShapeIndex.Cell cell = iterator.entry();
        List<S2ShapeIndex.S2ClippedShape> clippedShape = cell.clippedShapes();
        for (int k = 0; k < clippedShape.size(); k++) {
          int sid = cell.clipped(k).shapeId();
          result.add(parent.value(sid));
        }
      } else if (relation == S2ShapeIndex.CellRelation.SUBDIVIDED) {
        // Promising! the index has a child cell of iterator_.id()
        // (at which iterator_ is now positioned). Keep iterating until the
        // iterator is done OR we're no longer at a child cell of
        // iterator_.id(). The ordering of the iterator isn't guaranteed
        // anywhere in the documentation; however, this ordering would be
        // consistent with that of a Normalized S2CellUnion.
        while (!iterator.done() && cellId.contains(iterator.id())) {
          S2ShapeIndex.Cell cell = iterator.entry();
          List<S2ShapeIndex.S2ClippedShape> clippedShape = cell.clippedShapes();
          for (int k = 0; k < clippedShape.size(); k++) {
            int sid = cell.clipped(k).shapeId();
            result.add(parent.value(sid));
          }
          iterator.next();
        }
      }
      // DISJOINT: nothing to add
    }
  }

  private static int[] expand(int[] array, int newSize) {
    if (array == null) {
      return new int[newSize];
    } else if (array.length < newSize) {
      return Arrays.copyOf(array, newSize);
    }
    return array;
  }
}
