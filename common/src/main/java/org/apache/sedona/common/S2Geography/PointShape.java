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

import com.google.common.geometry.*;
import com.google.common.geometry.S2Edge;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Shape;
import java.util.List;

/**
 * Java equivalent of the C++ S2PointVectorShape: each point is a degenerate edge (start==end), one
 * chain per point.
 */
/** A degenerate S2Shape that represents exactly one point. */
public final class PointShape implements S2Shape {
  private final List<S2Point> point;

  public PointShape(List<S2Point> point) {
    this.point = point;
  }

  public int num_points() {
    return point.size();
  }

  @Override
  public int numEdges() {
    return num_points();
  }

  @Override
  public void getEdge(int index, MutableEdge result) {
    if (index != 0) {
      throw new IndexOutOfBoundsException("PointShape has exactly one edge");
    }
    result.set(point.get(index), point.get(index));
  }

  public S2Edge edge(int e) {
    return new S2Edge(point.get(e), point.get(e));
  }

  @Override
  public int dimension() {
    return 0;
  }

  @Override
  public boolean hasInterior() {
    return false;
  }

  @Override
  public boolean containsOrigin() {
    return false;
  }

  @Override
  public ReferencePoint getReferencePoint() {
    // hasInterior=false, contained()=false
    return ReferencePoint.create(point.get(0), false);
  }

  @Override
  public int numChains() {
    return 1;
  }

  @Override
  public int getChainStart(int chainId) {
    if (chainId != 0) {
      throw new IndexOutOfBoundsException("PointShape has exactly one chain");
    }
    return 0;
  }

  @Override
  public int getChainLength(int chainId) {
    if (chainId != 0) {
      throw new IndexOutOfBoundsException("PointShape has exactly one chain");
    }
    return 1;
  }

  @Override
  public void getChainEdge(int chainId, int offset, MutableEdge result) {
    if (chainId != 0 || offset != 0) {
      throw new IndexOutOfBoundsException("PointShape chainId and offset must both be 0");
    }
    result.set(point.get(chainId), point.get(chainId));
  }

  @Override
  public S2Point getChainVertex(int chainId, int offset) {
    if (chainId != 0 || offset != 0) {
      throw new IndexOutOfBoundsException("PointShape chainId and offset must both be 0");
    }
    return point.get(chainId);
  }
}
