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
import com.google.common.geometry.S2LatLngRect;
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
import org.locationtech.jts.index.strtree.Boundable;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * An in-memory index that answers k-nearest-neighbour queries between geographies. Neighbours are
 * ranked by {@link Functions#distance(Geography, Geography)}, the minimum distance on the sphere in
 * meters.
 *
 * <p>Every geography is stored in an STRtree under its latitude / longitude bounding rectangle. The
 * search walks that tree, but orders and prunes its nodes by the distance on the sphere between
 * rectangles: the planar distance the tree itself would use is wrong across the antimeridian and
 * near the poles.
 *
 * <p>All geographies have to be added before the first search. After that, any number of threads
 * can search the index at the same time.
 *
 * @param <T> the type of the item stored with each geography
 */
public final class GeographyKnnIndex<T> {

  /**
   * How much the distance between two rectangles is reduced before it is used as a lower bound,
   * about 6 meters on the ground. The rectangle distance and the distance between two geographies
   * are rounded differently, and can disagree by about 1e-7 radians where the arc sine they both
   * use is ill-conditioned (angles near 90 and 180 degrees). Without the slack that rounding can
   * prune a true neighbour.
   */
  private static final double LOWER_BOUND_SLACK_RADIANS = 1e-6;

  private final STRtree tree = new STRtree();
  private int size = 0;

  /**
   * Adds a geography and the item to return when it is found. A null or empty geography is skipped:
   * it is nobody's neighbour. So is a polygon whose rings enclose no area, which has no bounding
   * rectangle.
   */
  public void add(Geography geography, T item) {
    S2LatLngRect rect = bounds(geography);
    if (rect != null) {
      tree.insert(toEnvelope(rect), new Entry<>(geography, item));
      size++;
    }
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
   * <p>This is a best-first search. One queue holds tree nodes and geographies under a lower bound
   * of their distance, and geographies whose distance has been computed under that distance. A
   * geography with a computed distance that reaches the head of the queue is the nearest one not
   * yet returned, so a distance is only computed for geographies whose rectangle is near enough.
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

    PriorityQueue<Candidate> queue =
        new PriorityQueue<>(Comparator.comparingDouble(candidate -> candidate.distance));
    queue.add(new Candidate(0.0, tree.getRoot(), false));
    while (!queue.isEmpty()) {
      Candidate candidate = queue.poll();
      if (neighbors.size() >= k
          && !(includeTies && candidate.distance <= neighbors.get(k - 1).getDistance())) {
        // Nothing left in the queue is nearer than the k-th neighbour, or tied with it.
        break;
      }
      if (candidate.target instanceof AbstractNode) {
        for (Object child : ((AbstractNode) candidate.target).getChildBoundables()) {
          Envelope envelope = (Envelope) ((Boundable) child).getBounds();
          queue.add(new Candidate(lowerBound(queryRect, envelope), (Boundable) child, false));
        }
        continue;
      }
      @SuppressWarnings("unchecked")
      Entry<T> entry = (Entry<T>) ((ItemBoundable) candidate.target).getItem();
      if (candidate.isExact) {
        neighbors.add(new Neighbor<>(candidate.distance, entry.item));
      } else {
        Double distance = Functions.distance(query, entry.geography);
        if (distance != null && !distance.isNaN() && !distance.isInfinite()) {
          queue.add(new Candidate(distance, candidate.target, true));
        }
      }
    }
    return neighbors;
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
    int end = Math.min(k, sorted.size());
    while (includeTies
        && end < sorted.size()
        && sorted.get(end).getDistance() <= sorted.get(k - 1).getDistance()) {
      end++;
    }
    return new ArrayList<>(sorted.subList(0, end));
  }

  /**
   * The latitude / longitude bounding rectangle of a geography, as {@code
   * geography.region().getRectBound()} computes it. It follows the edges of the geography, and
   * reaches a pole when a polygon contains that pole.
   *
   * @return the rectangle, or null when the geography is null or empty
   */
  public static S2LatLngRect bounds(Geography geography) {
    if (geography == null
        || (geography instanceof WKBGeography && ((WKBGeography) geography).isEmpty())) {
      return null;
    }
    S2LatLngRect rect = geography.region().getRectBound();
    return rect.isEmpty() ? null : rect;
  }

  /**
   * The planar envelope, in degrees, that places a latitude / longitude rectangle in a planar
   * spatial index. A rectangle that crosses the antimeridian gets the full longitude range, because
   * a planar envelope cannot wrap around.
   */
  public static Envelope toEnvelope(S2LatLngRect rect) {
    if (rect.lng().isInverted()) {
      return new Envelope(-180.0, 180.0, rect.latLo().degrees(), rect.latHi().degrees());
    }
    return new Envelope(
        rect.lngLo().degrees(),
        rect.lngHi().degrees(),
        rect.latLo().degrees(),
        rect.latHi().degrees());
  }

  /** A distance in meters that no geography inside {@code envelope} can be nearer than. */
  private static double lowerBound(S2LatLngRect queryRect, Envelope envelope) {
    // The reverse of toEnvelope, clamped because degrees do not convert back exactly.
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
    double radians = queryRect.getDistance(new S2LatLngRect(lat, lng)).radians();
    return Math.max(0.0, radians - LOWER_BOUND_SLACK_RADIANS) * Haversine.AVG_EARTH_RADIUS;
  }

  private static void requirePositive(int k) {
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, but is " + k);
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
   * An entry of the search queue: a tree node or a geography, under a lower bound of its distance
   * in meters, or under the distance itself when {@code isExact} is set.
   */
  private static final class Candidate {
    final double distance;
    final Boundable target;
    final boolean isExact;

    Candidate(double distance, Boundable target, boolean isExact) {
      this.distance = distance;
      this.target = target;
      this.isExact = isExact;
    }
  }
}
