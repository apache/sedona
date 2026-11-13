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
package org.apache.sedona.common.Geography;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.apache.sedona.common.geography.GeographyKnnIndex;
import org.apache.sedona.common.geography.GeographyKnnIndex.Neighbor;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;

public class GeographyKnnIndexTest {

  private static Geography geography(String wkt) {
    try {
      return Constructors.geogFromWKT(wkt, 4326);
    } catch (ParseException e) {
      throw new IllegalArgumentException(wkt, e);
    }
  }

  /** Indexes the geographies under their position in the argument list. */
  private static GeographyKnnIndex<Integer> index(String... wkts) {
    GeographyKnnIndex.Builder<Integer> builder = GeographyKnnIndex.builder();
    for (int i = 0; i < wkts.length; i++) {
      builder.add(geography(wkts[i]), i);
    }
    return builder.build();
  }

  private static List<Integer> items(List<Neighbor<Integer>> neighbors) {
    return neighbors.stream().map(Neighbor::getItem).collect(Collectors.toList());
  }

  private static List<Double> distances(List<Neighbor<Integer>> neighbors) {
    return neighbors.stream().map(Neighbor::getDistance).collect(Collectors.toList());
  }

  /** The k smallest distances from the query, found by measuring every object. */
  private static List<Double> bruteForce(Geography query, List<Geography> objects, int k) {
    List<Double> all = new ArrayList<>();
    for (Geography object : objects) {
      all.add(Functions.distance(query, object));
    }
    Collections.sort(all);
    return all.subList(0, Math.min(k, all.size()));
  }

  private static void assertAgreesWithBruteForce(String[] objectWkts, String[] queryWkts, int k) {
    GeographyKnnIndex<Integer> objects = index(objectWkts);
    List<Geography> geographies =
        Arrays.stream(objectWkts)
            .map(GeographyKnnIndexTest::geography)
            .collect(Collectors.toList());
    for (String queryWkt : queryWkts) {
      Geography query = geography(queryWkt);
      assertEquals(
          queryWkt, bruteForce(query, geographies, k), distances(objects.nearest(query, k, false)));
    }
  }

  private static String randomPoint(Random random) {
    double lon = random.nextDouble() * 360 - 180;
    double lat = Math.toDegrees(Math.asin(random.nextDouble() * 2 - 1));
    return point(lon, lat);
  }

  /** WKT is written with a decimal point whatever the default locale of the JVM is. */
  private static String point(double lon, double lat) {
    return String.format(Locale.ROOT, "POINT(%.15f %.15f)", lon, lat);
  }

  /** A small square around a random centre that stays clear of the poles and the antimeridian. */
  private static String randomSquare(Random random) {
    double lon = random.nextDouble() * 350 - 175;
    double lat = random.nextDouble() * 160 - 80;
    double half = 0.05 + random.nextDouble() * 2;
    return String.format(
        Locale.ROOT,
        "POLYGON((%1$f %3$f, %2$f %3$f, %2$f %4$f, %1$f %4$f, %1$f %3$f))",
        lon - half,
        lon + half,
        lat - half,
        lat + half);
  }

  private static String[] generate(int count, java.util.function.Supplier<String> generator) {
    String[] wkts = new String[count];
    for (int i = 0; i < count; i++) {
      wkts[i] = generator.get();
    }
    return wkts;
  }

  @Test
  public void returnsNearestObjectsFirstWithDistanceInMeters() {
    GeographyKnnIndex<Integer> objects =
        index("POINT(3 0)", "POINT(1 0)", "POINT(-2 0)", "POINT(10 0)");
    Geography query = geography("POINT(0 0)");

    List<Neighbor<Integer>> neighbors = objects.nearest(query, 3, false);

    assertEquals(Arrays.asList(1, 2, 0), items(neighbors));
    assertEquals(
        Arrays.asList(
            Functions.distance(query, geography("POINT(1 0)")),
            Functions.distance(query, geography("POINT(-2 0)")),
            Functions.distance(query, geography("POINT(3 0)"))),
        distances(neighbors));
  }

  @Test
  public void agreesWithBruteForceOnRandomPointsOverTheWholeSphere() {
    Random random = new Random(42);
    assertAgreesWithBruteForce(
        generate(5000, () -> randomPoint(random)), generate(200, () -> randomPoint(random)), 7);
  }

  @Test
  public void agreesWithBruteForceOnRandomPolygons() {
    Random random = new Random(7);
    assertAgreesWithBruteForce(
        generate(1500, () -> randomSquare(random)), generate(60, () -> randomPoint(random)), 5);
  }

  @Test
  public void agreesWithBruteForceForPolygonQueries() {
    Random random = new Random(11);
    assertAgreesWithBruteForce(
        generate(1500, () -> randomPoint(random)), generate(60, () -> randomSquare(random)), 5);
  }

  @Test
  public void ranksByNearestEdgeRatherThanCentroid() {
    // The polygon's centroid is far east of the query, but its western edge is 1 degree away.
    GeographyKnnIndex<Integer> objects =
        index("POINT(-2 0)", "POLYGON((1 -1, 41 -1, 41 1, 1 1, 1 -1))");

    assertEquals(Arrays.asList(1), items(objects.nearest(geography("POINT(0 0)"), 1, false)));
  }

  @Test
  public void findsNeighboursAcrossTheAntimeridian() {
    // 2 degrees apart across the antimeridian, 9 degrees apart on the same side.
    GeographyKnnIndex<Integer> objects = index("POINT(170 0)", "POINT(-179 0)");

    assertEquals(Arrays.asList(1, 0), items(objects.nearest(geography("POINT(179 0)"), 2, false)));
  }

  @Test
  public void findsObjectsThatCrossTheAntimeridian() {
    // The polygon spans 178E to 178W, so it contains the query. Its planar envelope does not say
    // which way round it goes.
    GeographyKnnIndex<Integer> objects =
        index("POINT(170 0)", "POLYGON((178 -1, -178 -1, -178 1, 178 1, 178 -1))");

    List<Neighbor<Integer>> neighbors = objects.nearest(geography("POINT(180 0)"), 1, false);

    assertEquals(Arrays.asList(1), items(neighbors));
    assertEquals(0.0, neighbors.get(0).getDistance(), 0.0);
  }

  @Test
  public void findsNeighboursAcrossThePole() {
    // Both objects are 180 degrees of longitude away from the query. The first is 2 degrees of
    // arc away over the pole, the second is 9 degrees away on the query's own meridian.
    GeographyKnnIndex<Integer> objects = index("POINT(180 89)", "POINT(0 80)");

    assertEquals(Arrays.asList(0, 1), items(objects.nearest(geography("POINT(0 89)"), 2, false)));
  }

  @Test
  public void ordersBySphereDistanceRatherThanDegrees() {
    // At 60N a degree of longitude is half as long as a degree of latitude, so the object that
    // is 3 degrees east is nearer than the one that is 2 degrees north.
    GeographyKnnIndex<Integer> objects = index("POINT(0 62)", "POINT(3 60)");

    assertEquals(Arrays.asList(1, 0), items(objects.nearest(geography("POINT(0 60)"), 2, false)));
  }

  @Test
  public void findsAPolygonThatCoversMoreThanItsVerticesSuggest() {
    // The ring runs along the equator, so the polygon is a hemisphere. Distance measures it as
    // the northern one: the query near the north pole is inside it.
    Geography hemisphere = geography("POLYGON((0 -1e-14,120 -1e-14,-120 -1e-14,0 -1e-14))");
    Geography query = geography("POINT(0 89)");
    assertEquals(0.0, Functions.distance(query, hemisphere), 0.0);

    GeographyKnnIndex<Integer> objects =
        GeographyKnnIndex.<Integer>builder()
            .add(geography("POINT(0 88)"), 0)
            .add(hemisphere, 1)
            .build();
    List<Neighbor<Integer>> neighbors = objects.nearest(query, 1, false);

    assertEquals(Arrays.asList(1), items(neighbors));
    assertEquals(0.0, neighbors.get(0).getDistance(), 0.0);
  }

  @Test
  public void boundsCoverThePolesThatAPolygonContains() {
    S2LatLngRect north = GeographyKnnIndex.bounds(geography("POLYGON((0 80,120 80,-120 80,0 80))"));
    assertEquals(Math.PI / 2, north.latHi().radians(), 0.0);
    assertTrue(north.lng().isFull());
    assertEquals(80.0, north.latLo().degrees(), 1.0);

    S2LatLngRect south =
        GeographyKnnIndex.bounds(geography("POLYGON((0 -80,-120 -80,120 -80,0 -80))"));
    assertEquals(-Math.PI / 2, south.latLo().radians(), 0.0);
    assertTrue(south.lng().isFull());
    assertEquals(-80.0, south.latHi().degrees(), 1.0);
  }

  @Test
  public void indexesCollapsedPolygonsThatHaveADistance() {
    // Rings that enclose no area still have edges, and distance measures to those edges.
    String[] collapsed = {
      "POLYGON((0 0, 1 1, 1 1, 0 0))",
      "POLYGON((3 3, 3 3, 3 3, 3 3))",
      "POLYGON((5 0, 6 0, 7 0, 5 0))"
    };
    Geography query = geography("POINT(0.5 0.5)");
    for (String wkt : collapsed) {
      Double distance = Functions.distance(query, geography(wkt));
      assertTrue(wkt, distance != null && !distance.isInfinite() && !distance.isNaN());
    }

    assertEquals(3, index(collapsed).size());
    assertAgreesWithBruteForce(collapsed, new String[] {"POINT(0.5 0.5)", "POINT(4 2)"}, 3);
  }

  @Test
  public void agreesWithBruteForceNearTheAntipodeOfTheQuery() {
    // Angles close to 180 degrees are where rectangle distances are least precise. The objects
    // sit within centimeters to meters of the antipode, at distances that differ by less than
    // that imprecision.
    Random random = new Random(3);
    for (int trial = 0; trial < 300; trial++) {
      double lon = random.nextDouble() * 340 - 170;
      double lat = random.nextDouble() * 160 - 80;
      double spread = Math.pow(10, -3 - random.nextDouble() * 6);
      double antipodeLon = lon > 0 ? lon - 180 : lon + 180;
      String[] objects =
          generate(
              30,
              () ->
                  point(
                      antipodeLon + (random.nextDouble() - 0.5) * spread,
                      -lat + (random.nextDouble() - 0.5) * spread));
      assertAgreesWithBruteForce(objects, new String[] {point(lon, lat)}, 3);
    }
  }

  @Test
  public void agreesWithBruteForceAQuarterOfTheSphereAway() {
    // Every object on a meridian is about 90 degrees from a query on the equator a quarter turn
    // away, which is where the distance to the side of a rectangle is least precise.
    Random random = new Random(5);
    for (int trial = 0; trial < 100; trial++) {
      double lon = random.nextDouble() * 80 - 40;
      double spread = Math.pow(10, -3 - random.nextDouble() * 6);
      String[] objects = generate(200, () -> point(lon + 90, random.nextDouble() * 2 - 1));
      String query =
          point(lon + (random.nextDouble() - 0.5) * spread, (random.nextDouble() - 0.5) * spread);
      assertAgreesWithBruteForce(objects, new String[] {query}, 3);
    }
  }

  @Test
  public void returnsEveryObjectWhenKExceedsTheNumberOfObjects() {
    GeographyKnnIndex<Integer> objects = index("POINT(3 0)", "POINT(1 0)");

    assertEquals(Arrays.asList(1, 0), items(objects.nearest(geography("POINT(0 0)"), 10, true)));
  }

  @Test
  public void returnsObjectsTiedAtTheKthDistanceOnlyWhenTiesAreIncluded() {
    GeographyKnnIndex<Integer> objects =
        index("POINT(1 0)", "POINT(1 0)", "POINT(1 0)", "POINT(0.5 0)", "POINT(2 0)");
    Geography query = geography("POINT(0 0)");

    List<Integer> withTies = items(objects.nearest(query, 2, true));
    Collections.sort(withTies);
    assertEquals(Arrays.asList(0, 1, 2, 3), withTies);

    List<Integer> withoutTies = items(objects.nearest(query, 2, false));
    assertEquals(2, withoutTies.size());
    assertEquals(Integer.valueOf(3), withoutTies.get(0));
  }

  @Test
  public void ignoresNullAndEmptyObjects() {
    GeographyKnnIndex<Integer> objects =
        GeographyKnnIndex.<Integer>builder()
            .add(geography("POINT EMPTY"), 0)
            .add(geography("POINT(1 0)"), 1)
            .add(null, 2)
            .add(geography("POLYGON EMPTY"), 3)
            .add(geography("LINESTRING EMPTY"), 4)
            .build();

    assertEquals(1, objects.size());
    assertEquals(Arrays.asList(1), items(objects.nearest(geography("POINT(0 0)"), 5, false)));
  }

  @Test
  public void returnsNothingForNullOrEmptyQuery() {
    GeographyKnnIndex<Integer> objects = index("POINT(1 0)");

    assertTrue(objects.nearest(null, 5, false).isEmpty());
    assertTrue(objects.nearest(geography("POINT EMPTY"), 5, false).isEmpty());
    assertTrue(objects.nearest(geography("POLYGON EMPTY"), 5, false).isEmpty());
  }

  @Test
  public void anIndexWithoutObjectsIsEmpty() {
    GeographyKnnIndex<Integer> objects = index("POINT EMPTY");

    assertTrue(objects.isEmpty());
    assertEquals(0, objects.size());
    assertTrue(objects.nearest(geography("POINT(0 0)"), 1, false).isEmpty());
    assertFalse(index("POINT(0 0)").isEmpty());
  }

  @Test
  public void rejectsKBelowOne() {
    GeographyKnnIndex<Integer> objects = index("POINT(1 0)");

    assertThrows(
        IllegalArgumentException.class, () -> objects.nearest(geography("POINT(0 0)"), 0, false));
  }

  @Test
  public void mergeKeepsTheKNearestOfTheUnion() {
    List<Neighbor<Integer>> a = neighbors(1.0, 4.0, 4.0);
    List<Neighbor<Integer>> b = neighbors(2.0, 4.0, 9.0);

    assertEquals(Arrays.asList(1.0, 2.0, 4.0), distances(GeographyKnnIndex.merge(a, b, 3, false)));
    assertEquals(
        Arrays.asList(1.0, 2.0, 4.0, 4.0, 4.0), distances(GeographyKnnIndex.merge(a, b, 3, true)));
    assertEquals(
        Arrays.asList(1.0, 2.0, 4.0, 4.0, 4.0, 9.0),
        distances(GeographyKnnIndex.merge(a, b, 10, false)));
  }

  @Test
  public void mergedNeighboursSurviveJavaSerialization() throws Exception {
    List<Neighbor<Integer>> merged =
        GeographyKnnIndex.merge(neighbors(1.0, 4.0), neighbors(2.0), 2, false);

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(merged);
    }
    List<Neighbor<Integer>> copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      @SuppressWarnings("unchecked")
      List<Neighbor<Integer>> read = (List<Neighbor<Integer>>) in.readObject();
      copy = read;
    }

    assertEquals(Arrays.asList(1.0, 2.0), distances(copy));
    assertEquals(Arrays.asList(1, 2), items(copy));
  }

  @Test
  public void boundsIsTheLatLngRectangleOfTheGeography() {
    S2LatLngRect rect = GeographyKnnIndex.bounds(geography("LINESTRING(10 20, 30 40)"));

    assertEquals(20.0, rect.latLo().degrees(), 1e-6);
    assertEquals(40.0, rect.latHi().degrees(), 1e-6);
    assertEquals(10.0, rect.lngLo().degrees(), 1e-6);
    assertEquals(30.0, rect.lngHi().degrees(), 1e-6);
    assertNull(GeographyKnnIndex.bounds(null));
    assertNull(GeographyKnnIndex.bounds(geography("POINT EMPTY")));
    assertNull(GeographyKnnIndex.bounds(geography("POLYGON EMPTY")));
  }

  @Test
  public void envelopeOfARectangleAcrossTheAntimeridianCoversEveryLongitude() {
    assertEnvelope(10, 30, 20, 40, GeographyKnnIndex.toEnvelope(rect(20, 10, 40, 30)));
    // From 170E eastwards to 170W.
    assertEnvelope(-180, 180, -5, 5, GeographyKnnIndex.toEnvelope(rect(-5, 170, 5, -170)));
  }

  private static S2LatLngRect rect(double latLo, double lngLo, double latHi, double lngHi) {
    return new S2LatLngRect(S2LatLng.fromDegrees(latLo, lngLo), S2LatLng.fromDegrees(latHi, lngHi));
  }

  private static void assertEnvelope(
      double minX, double maxX, double minY, double maxY, Envelope actual) {
    assertEquals(minX, actual.getMinX(), 1e-9);
    assertEquals(maxX, actual.getMaxX(), 1e-9);
    assertEquals(minY, actual.getMinY(), 1e-9);
    assertEquals(maxY, actual.getMaxY(), 1e-9);
  }

  /** Neighbours at the given distances, each carrying its distance as the item. */
  private static List<Neighbor<Integer>> neighbors(double... distances) {
    List<Neighbor<Integer>> neighbors = new ArrayList<>();
    for (double distance : distances) {
      neighbors.add(new Neighbor<>(distance, (int) distance));
    }
    return neighbors;
  }
}
