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
package org.apache.sedona.common.geography;

import com.google.common.geometry.R1Interval;
import com.google.common.geometry.S1Interval;
import com.google.common.geometry.S2EdgeUtil;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Shape;
import com.google.common.geometry.S2ShapeUtil;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.WKBGeography;
import org.apache.sedona.common.sphere.Haversine;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * An in-memory index that answers k-nearest-neighbour queries between geographies. Neighbours are
 * ranked by {@link Functions#distance(Geography, Geography)}, the minimum distance on the sphere in
 * meters.
 *
 * <p>Every geography is indexed by its latitude / longitude bounding rectangle in an STRtree. The
 * tree is only used as a hierarchy of rectangles: the search orders and prunes its nodes by the
 * distance on the sphere between the query's rectangle and the node's rectangle, which never
 * exceeds the distance to any geography under that node. A planar distance between the same
 * rectangles would be wrong across the antimeridian and near the poles.
 *
 * <p>An index is immutable once built, so any number of threads can search it at the same time.
 *
 * @param <T> the type of the item stored with each geography
 */
public final class GeographyKnnIndex<T> {

  /**
   * How much the distance between two rectangles is reduced before it is used as a lower bound,
   * about 6 meters on the ground.
   *
   * <p>The rectangle distance and the distance between two geographies are computed along different
   * floating-point paths, and both go through an arc sine that is ill-conditioned for some angles:
   * near 180 degrees for the distance between two points or to the end of an edge, and near 90
   * degrees for the distance to the inside of an edge. There a relative error of a few ulps in the
   * argument becomes an absolute error of up to about 4e-8 radians in the angle, so the two values
   * can disagree by about 1e-7 radians. The slack is ten times that, so that rounding can never
   * push the bound above the distance of a geography inside the rectangle. It only costs a few
   * extra distance computations for geographies within this margin of the k-th neighbour.
   */
  private static final double LOWER_BOUND_SLACK_RADIANS = 1e-6;

  private static final S2Point NORTH_POLE = new S2Point(0, 0, 1);
  private static final S2Point SOUTH_POLE = new S2Point(0, 0, -1);

  private static final Comparator<Candidate> CANDIDATE_ORDER =
      Comparator.comparingDouble(candidate -> candidate.key);

  private final STRtree tree;
  private final int size;

  private GeographyKnnIndex(STRtree tree, int size) {
    this.tree = tree;
    this.size = size;
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  /** The number of geographies in the index. */
  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Finds the k geographies nearest to the query, nearest first.
   *
   * <p>This is a best-first search over a single priority queue that holds tree nodes and
   * geographies keyed by a lower bound of their distance, and geographies whose exact distance is
   * known keyed by that distance. A geography with a known distance that reaches the head of the
   * queue is therefore the nearest one not yet returned.
   *
   * @param query the geography to search around; a null or empty query has no neighbours
   * @param k the number of neighbours to return, at least 1
   * @param includeTies whether to also return every further geography at exactly the k-th distance
   * @return at most k neighbours, or more with {@code includeTies}; fewer when the index holds
   *     fewer than k geographies
   */
  public List<Neighbor<T>> nearest(Geography query, int k, boolean includeTies) {
    requirePositive(k);
    List<Neighbor<T>> neighbors = new ArrayList<>();
    S2LatLngRect queryRect = bounds(query);
    if (queryRect == null || size == 0) {
      return neighbors;
    }

    PriorityQueue<Candidate> queue = new PriorityQueue<>(CANDIDATE_ORDER);
    queue.add(new Candidate(0.0, tree.getRoot(), false));
    while (!queue.isEmpty()) {
      Candidate candidate = queue.poll();
      if (neighbors.size() >= k
          && !(includeTies && candidate.key <= neighbors.get(k - 1).getDistance())) {
        // Nothing left in the queue is nearer than the k-th neighbour, or tied with it.
        break;
      }
      if (candidate.target instanceof AbstractNode) {
        expand((AbstractNode) candidate.target, queryRect, queue);
      } else {
        @SuppressWarnings("unchecked")
        Entry<T> entry = (Entry<T>) candidate.target;
        if (candidate.exact) {
          neighbors.add(new Neighbor<>(candidate.key, entry.item));
        } else {
          measure(query, entry, queue);
        }
      }
    }
    return neighbors;
  }

  /** Queues the children of a tree node, keyed by a lower bound of their distance. */
  private static void expand(
      AbstractNode node, S2LatLngRect queryRect, PriorityQueue<Candidate> queue) {
    for (Object child : node.getChildBoundables()) {
      if (child instanceof ItemBoundable) {
        ItemBoundable item = (ItemBoundable) child;
        queue.add(
            new Candidate(
                lowerBound(queryRect, (Envelope) item.getBounds()), item.getItem(), false));
      } else {
        AbstractNode childNode = (AbstractNode) child;
        queue.add(
            new Candidate(
                lowerBound(queryRect, (Envelope) childNode.getBounds()), childNode, false));
      }
    }
  }

  /** Queues a geography again, keyed by its exact distance from the query. */
  private static void measure(Geography query, Entry<?> entry, PriorityQueue<Candidate> queue) {
    Double distance = Functions.distance(query, entry.geography);
    if (distance != null && !distance.isNaN() && !distance.isInfinite()) {
      queue.add(new Candidate(distance, entry, true));
    }
  }

  /**
   * Merges the nearest neighbours that two disjoint sets of geographies produced for the same query
   * into the nearest neighbours of their union, as {@link #nearest} would return them for an index
   * over both sets.
   *
   * @param a neighbours from the first set, in any order
   * @param b neighbours from the second set, in any order
   * @param k the number of neighbours to keep, at least 1
   * @param includeTies whether to also keep every further neighbour at exactly the k-th distance
   */
  public static <T> List<Neighbor<T>> merge(
      List<Neighbor<T>> a, List<Neighbor<T>> b, int k, boolean includeTies) {
    requirePositive(k);
    List<Neighbor<T>> sorted = new ArrayList<>(a.size() + b.size());
    sorted.addAll(a);
    sorted.addAll(b);
    sorted.sort(Comparator.comparingDouble(Neighbor::getDistance));
    if (sorted.size() <= k) {
      return sorted;
    }
    int end = k;
    if (includeTies) {
      double kthDistance = sorted.get(k - 1).getDistance();
      while (end < sorted.size() && sorted.get(end).getDistance() <= kthDistance) {
        end++;
      }
    }
    return new ArrayList<>(sorted.subList(0, end));
  }

  /**
   * The latitude / longitude bounding rectangle of a geography.
   *
   * <p>The rectangle covers the shapes that {@link Functions#distance(Geography, Geography)}
   * measures, which is what makes it safe to prune with. It is computed from those shapes, edge by
   * edge, and extended to a pole when a polygon contains that pole.
   *
   * @return the rectangle, or null when the geography is null or has nothing to measure a distance
   *     to
   */
  public static S2LatLngRect bounds(Geography geography) {
    if (geography == null) {
      return null;
    }
    if (geography instanceof WKBGeography) {
      WKBGeography wkb = (WKBGeography) geography;
      if (wkb.isEmpty()) {
        return null;
      }
      if (wkb.isPoint()) {
        // The same conversion as WKBGeography.extractPoint(), which distance uses for points.
        S2Point point = S2LatLng.fromDegrees(wkb.getPointY(), wkb.getPointX()).toPoint();
        return S2LatLngRect.fromPoint(new S2LatLng(point));
      }
    }

    S2LatLngRect bound = S2LatLngRect.empty();
    boolean containsNorthPole = false;
    boolean containsSouthPole = false;
    S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
    for (S2Shape shape : Functions.toShapeIndex(geography).shapeIndex.getShapes()) {
      if (shape == null) {
        continue;
      }
      for (int chain = 0; chain < shape.numChains(); chain++) {
        // A bounder follows one chain of edges, including the way each edge bulges away from the
        // straight line between its ends in latitude / longitude space.
        S2EdgeUtil.RectBounder bounder = new S2EdgeUtil.RectBounder();
        int length = shape.getChainLength(chain);
        for (int offset = 0; offset < length; offset++) {
          shape.getChainEdge(chain, offset, edge);
          if (offset == 0) {
            bounder.addPoint(edge.getStart());
          }
          if (!edge.getEnd().equalsPoint(edge.getStart())) {
            bounder.addPoint(edge.getEnd());
          }
        }
        if (length > 0) {
          bound = bound.union(bounder.getBound());
        }
      }
      if (shape.dimension() == 2) {
        // The interior of a polygon reaches further than its edges only around a pole.
        containsNorthPole |= S2ShapeUtil.containsBruteForce(shape, NORTH_POLE);
        containsSouthPole |= S2ShapeUtil.containsBruteForce(shape, SOUTH_POLE);
      }
    }
    if (containsNorthPole || containsSouthPole) {
      double latLo = bound.isEmpty() ? Math.PI / 2 : bound.lat().lo();
      double latHi = bound.isEmpty() ? -Math.PI / 2 : bound.lat().hi();
      bound =
          new S2LatLngRect(
              new R1Interval(
                  containsSouthPole ? -Math.PI / 2 : latLo,
                  containsNorthPole ? Math.PI / 2 : latHi),
              S1Interval.full());
    }
    return bound.isEmpty() ? null : bound;
  }

  /**
   * The planar envelope, in degrees, that places a latitude / longitude rectangle in a planar
   * spatial index. A rectangle that crosses the antimeridian gets the full longitude range, because
   * a planar envelope cannot wrap around.
   */
  public static Envelope toEnvelope(S2LatLngRect rect) {
    double lngLo = -180.0;
    double lngHi = 180.0;
    if (!rect.lng().isInverted()) {
      lngLo = rect.lngLo().degrees();
      lngHi = rect.lngHi().degrees();
    }
    return new Envelope(lngLo, lngHi, rect.latLo().degrees(), rect.latHi().degrees());
  }

  private static S2LatLngRect toRect(Envelope envelope) {
    R1Interval lat =
        new R1Interval(
            Math.max(Math.toRadians(envelope.getMinY()), -Math.PI / 2),
            Math.min(Math.toRadians(envelope.getMaxY()), Math.PI / 2));
    S1Interval lng =
        envelope.getMinX() <= -180.0 && envelope.getMaxX() >= 180.0
            ? S1Interval.full()
            : new S1Interval(
                Math.max(Math.toRadians(envelope.getMinX()), -Math.PI),
                Math.min(Math.toRadians(envelope.getMaxX()), Math.PI));
    return new S2LatLngRect(lat, lng);
  }

  /** A distance in meters that no geography inside {@code envelope} can be nearer than. */
  private static double lowerBound(S2LatLngRect queryRect, Envelope envelope) {
    double radians = queryRect.getDistance(toRect(envelope)).radians();
    return Math.max(0.0, radians - LOWER_BOUND_SLACK_RADIANS) * Haversine.AVG_EARTH_RADIUS;
  }

  private static void requirePositive(int k) {
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, but is " + k);
    }
  }

  /** Collects the geographies of a {@link GeographyKnnIndex}. A builder builds one index. */
  public static final class Builder<T> {
    private STRtree tree = new STRtree();
    private int size = 0;

    private Builder() {}

    /**
     * Adds a geography and the item to return when it is found. A null or empty geography is
     * skipped: it is nobody's neighbour.
     */
    public Builder<T> add(Geography geography, T item) {
      if (tree == null) {
        throw new IllegalStateException("The index has already been built");
      }
      S2LatLngRect rect = bounds(geography);
      if (rect != null) {
        tree.insert(toEnvelope(rect), new Entry<>(geography, item));
        size++;
      }
      return this;
    }

    public GeographyKnnIndex<T> build() {
      if (tree == null) {
        throw new IllegalStateException("The index has already been built");
      }
      if (size > 0) {
        tree.build();
      }
      GeographyKnnIndex<T> index = new GeographyKnnIndex<>(tree, size);
      tree = null;
      return index;
    }
  }

  /**
   * A geography found by {@link #nearest}: the item it was added with, and its distance from the
   * query in meters.
   */
  public static final class Neighbor<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final double distance;
    private final T item;

    public Neighbor(double distance, T item) {
      this.distance = distance;
      this.item = item;
    }

    public double getDistance() {
      return distance;
    }

    public T getItem() {
      return item;
    }
  }

  private static final class Entry<T> {
    final Geography geography;
    final T item;

    Entry(Geography geography, T item) {
      this.geography = geography;
      this.item = item;
    }
  }

  /**
   * An entry of the search queue. {@code target} is a tree node or an {@link Entry}. {@code key} is
   * the exact distance in meters when {@code exact} is set, and a lower bound of it otherwise.
   */
  private static final class Candidate {
    final double key;
    final Object target;
    final boolean exact;

    Candidate(double key, Object target, boolean exact) {
      this.key = key;
      this.target = target;
      this.exact = exact;
    }
  }
}
