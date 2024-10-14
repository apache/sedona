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
package org.apache.sedona.common;

import static org.junit.Assert.assertThrows;

import com.uber.h3core.exceptions.H3Exception;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.sedona.common.utils.H3Utils;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class H3UtilTest {
  public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private Coordinate[] coordArray(double... coordValues) {
    Coordinate[] coords = new Coordinate[(int) (coordValues.length / 2)];
    for (int i = 0; i < coordValues.length; i += 2) {
      coords[(int) (i / 2)] = new Coordinate(coordValues[i], coordValues[i + 1]);
    }
    return coords;
  }

  public static Polygon[] cellsToJTSPolygons(List<Long> cells) {
    return H3Utils.h3.cellsToMultiPolygon(cells, true).stream()
        .map(
            shellHoles -> {
              List<LinearRing> rings =
                  shellHoles.stream()
                      .map(
                          shell ->
                              GEOMETRY_FACTORY.createLinearRing(
                                  shell.stream()
                                      .map(latLng -> new Coordinate(latLng.lng, latLng.lat))
                                      .toArray(Coordinate[]::new)))
                      .collect(Collectors.toList());
              LinearRing shell = rings.remove(0);
              if (rings.isEmpty()) {
                return GEOMETRY_FACTORY.createPolygon(shell);
              } else {
                return GEOMETRY_FACTORY.createPolygon(shell, rings.toArray(new LinearRing[0]));
              }
            })
        .toArray(Polygon[]::new);
  }

  @Test
  public void pointToCell() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(0.0, 1.0));
    long cellId = H3Utils.coordinateToCell(point.getCoordinate(), 13);
    // the reversed polygon from cell should be able to cover point
    assert cellsToJTSPolygons(Collections.singletonList(cellId))[0].contains(point);
  }

  @Test
  public void polygonToCells() throws ParseException {
    Polygon target =
        (Polygon)
            new WKTReader()
                .read(
                    "POLYGON((-122.5062527079981 37.77488349032218,-122.5007595439356 37.732537587382446,-122.48839992479498 37.67223433509411,-122.3963894267481 37.69505755123785,-122.39501613573248 37.788450765773675,-122.46505397752935 37.79984535251623,-122.5062527079981 37.77488349032218))");
    List<Long> cells = H3Utils.polygonToCells(target, 7, true);
    // the cells of mbr must include all the cells' cells
    Polygon mbr = (Polygon) target.getEnvelope();
    List<Long> cellsMBR = H3Utils.polygonToCells(mbr, 7, true);
    assert new HashSet<>(cellsMBR).containsAll(cells);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for conjective
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert cellPolygon.contains(target);
  }

  @Test
  public void polygonToCellsNoMatch() throws ParseException {
    Polygon target =
        (Polygon)
            new WKTReader()
                .read(
                    "POLYGON ((-122.3852447 47.5606571, -122.385248 47.5605286, -122.385326 47.5605296, -122.3853281 47.5604473, -122.3852624 47.5604465, -122.3852646 47.5603628, -122.3853411 47.5603637, -122.3853543 47.5598464, -122.3846514 47.5598382, -122.3846307 47.5606499, -122.3852447 47.5606571))");
    Point centroid = target.getCentroid();
    long centroidCell = H3Utils.coordinateToCell(centroid.getCoordinate(), 8);
    List<Long> cells = H3Utils.polygonToCells(target, 8, false);
    assert cells.size() == 1 && cells.get(0) == centroidCell;
  }

  @Test
  public void polygonToCellsNoMatchFullCover() throws ParseException {
    Polygon target =
        (Polygon)
            new WKTReader()
                .read(
                    "POLYGON ((-122.3852447 47.5606571, -122.385248 47.5605286, -122.385326 47.5605296, -122.3853281 47.5604473, -122.3852624 47.5604465, -122.3852646 47.5603628, -122.3853411 47.5603637, -122.3853543 47.5598464, -122.3846514 47.5598382, -122.3846307 47.5606499, -122.3852447 47.5606571))");
    List<Long> cells = H3Utils.polygonToCells(target, 7, true);
    // the cells of mbr must include all the cells' cells
    Polygon mbr = (Polygon) target.getEnvelope();
    List<Long> cellsMBR = H3Utils.polygonToCells(mbr, 7, true);
    assert new HashSet<>(cellsMBR).containsAll(cells);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for conjective
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert cellPolygon.contains(target);
  }

  @Test
  public void polygonToCellsNoFullCover() throws ParseException {
    Polygon target =
        (Polygon)
            new WKTReader()
                .read(
                    "POLYGON((-122.5062527079981 37.77488349032218,-122.5007595439356 37.732537587382446,-122.48839992479498 37.67223433509411,-122.3963894267481 37.69505755123785,-122.39501613573248 37.788450765773675,-122.46505397752935 37.79984535251623,-122.5062527079981 37.77488349032218))");
    List<Long> cells = H3Utils.polygonToCells(target, 7, false);
    // the cells of fullCover should include all the cells non-full cover
    List<Long> cellsFullCover = H3Utils.polygonToCells(target, 7, true);
    assert new HashSet<>(cellsFullCover).containsAll(cells);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for conjective
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert !cellPolygon.contains(target);
    assert cellPolygon.intersects(target);
  }

  @Test
  public void polygonOnPentagonToCells() throws ParseException {
    Polygon target =
        (Polygon)
            new WKTReader()
                .read(
                    "POLYGON((10.47311531687734 64.74411329535432,10.427796713361715 64.67488499523719,10.543153158674215 64.64902571378299,10.674989096174215 64.68017135781035,10.595338217267965 64.76636949080041,10.47311531687734 64.74411329535432))");
    List<Long> cells = H3Utils.polygonToCells(target, 6, true);
    // the cells of mbr must include all the cells' cells
    Polygon mbr = (Polygon) target.getEnvelope();
    List<Long> cellsMBR = H3Utils.polygonToCells(mbr, 6, true);
    assert new HashSet<>(cellsMBR).containsAll(cells);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for conjective
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert cellPolygon.contains(target);
  }

  @Test
  public void polygonWithHolesToCells() throws ParseException {
    Polygon target =
        (Polygon)
            new WKTReader()
                .read(
                    "POLYGON((-122.49150256862254 37.76006117790207,-122.46506671657176 37.78394270449753,-122.41906146754832 37.77715896429647,-122.3905656789741 37.74187348046013,-122.42524127711863 37.71254658823643,-122.47811298122019 37.72666839340869,-122.49150256862254 37.76006117790207), (-122.44927386989207 37.73752948850401,-122.42798785914988 37.74920338883217,-122.42730121364207 37.76196112716144,-122.46335010280222 37.76250396084337,-122.44927386989207 37.73752948850401)))");
    List<Long> cells = H3Utils.polygonToCells(target, 9, true);
    // the cells of mbr must include all the cells' cells
    Polygon mbr = (Polygon) target.getEnvelope();
    List<Long> cellsMBR = H3Utils.polygonToCells(mbr, 9, true);
    assert new HashSet<>(cellsMBR).containsAll(cells);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for connected
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert cellPolygon.contains(target);
    // catch exception
    Exception exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              H3Utils.polygonToCells(
                  (Polygon) new WKTReader().read("POLYGON ((75 29, 77 29, 75 29))"), 7, true);
            });
    assert exception.toString().contains("POLYGON ((75 29, 77 29, 75 29))");
  }

  @Test
  public void approxPathCells() {
    LineString target =
        GEOMETRY_FACTORY.createLineString(coordArray(-122.532, 37.771, -122.589, 37.743));
    List<Long> cells = H3Utils.lineStringToCells(target, 8, false);
    List<Long> approxCells =
        H3Utils.approxPathCells(target.getCoordinateN(0), target.getCoordinateN(1), 8, true);
    // after truncation, removed redundant cells
    assert approxCells.size() == cells.size();
  }

  @Test
  public void approxPathCellsPentagon() {
    LineString target =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                58.174758948493505, 10.427371502467615, 58.1388817207103, 10.469490838693966));
    // assert use h3 native function will throw error
    assertThrows(
        H3Exception.class,
        () -> {
          H3Utils.h3.gridPathCells(
              H3Utils.coordinateToCell(target.getCoordinateN(0), 10),
              H3Utils.coordinateToCell(target.getCoordinateN(1), 10));
        });
    List<Long> cells =
        H3Utils.approxPathCells(target.getCoordinateN(0), target.getCoordinateN(1), 10, true);
    Long pentagonCell =
        cells.stream().filter(H3Utils.h3::isPentagon).collect(Collectors.toList()).get(0);
    Set<Long> h3Cells =
        new HashSet<>(
            H3Utils.h3.gridPathCells(
                H3Utils.coordinateToCell(target.getCoordinateN(0), 10), pentagonCell));
    h3Cells.addAll(
        H3Utils.h3.gridPathCells(
            pentagonCell, H3Utils.coordinateToCell(target.getCoordinateN(1), 10)));
    assert cells.size() == h3Cells.size();
  }

  @Test
  public void lineStringToCells() {
    LineString target =
        GEOMETRY_FACTORY.createLineString(
            coordArray(-122.589, 37.743, -122.532, 37.771, -122.423, 37.742, -122.323, 37.842));
    List<Long> cells = H3Utils.lineStringToCells(target, 7, true);
    // the cells of mbr must include all the cells' cells
    Polygon mbr = (Polygon) target.getEnvelope();
    List<Long> cellsMBR = H3Utils.polygonToCells(mbr, 7, true);
    assert new HashSet<>(cellsMBR).containsAll(cells);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for conjective
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert cellPolygon.contains(target);
  }

  @Test
  public void lineStringCrossPentagonToCells() {
    // this line cross the pentagon
    LineString target =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                10.466683443818056, 64.72575037391769, 10.595772799286806, 64.66235272011512));
    // first this case throw error when call native h3 functions
    assertThrows(
        H3Exception.class,
        () ->
            H3Utils.h3.gridPathCells(
                H3Utils.coordinateToCell(target.getCoordinateN(0), 6),
                H3Utils.coordinateToCell(target.getCoordinateN(1), 6)));
    List<Long> cells = H3Utils.lineStringToCells(target, 6, true);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for conjective
    // cells
    Polygon cellPolygon = cellsToJTSPolygons(cells)[0];
    // the reversed polygons from cells must all intersect with the polygon, this means the indexing
    // geographically make sense
    assert cellPolygon.contains(target);
  }

  @Test
  public void lineStringCrossPentagonToCellsNonFullCover() {
    // this line cross the pentagon
    LineString target =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                10.466683443818056, 64.72575037391769, 10.595772799286806, 64.66235272011512));
    List<Long> cellsFullCover = H3Utils.lineStringToCells(target, 6, true);
    List<Long> cellsNonFullCover = H3Utils.lineStringToCells(target, 6, false);
    // generate the reversed polygons from cells, the h3 generate 1 compacted polygon for adjacent
    // cells
    List<Long> approxCells =
        H3Utils.approxPathCells(target.getCoordinateN(0), target.getCoordinateN(1), 6, true);
    assert approxCells.equals(cellsNonFullCover);
    assert !approxCells.equals(cellsFullCover);
  }
}
