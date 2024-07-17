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
package org.apache.sedona.common.utils;

import com.uber.h3core.H3Core;
import com.uber.h3core.exceptions.H3Exception;
import com.uber.h3core.util.LatLng;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

public class H3Utils {
  public static H3Core h3;
  public static Map<Integer, Double> cellSizeMap;

  public static class H3UtilException extends RuntimeException {
    public H3UtilException(Throwable cause) {
      super(cause);
    }

    public H3UtilException(String message) {
      super(message);
    }

    public H3UtilException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  static {
    try {
      h3 = H3Core.newInstance();
    } catch (IOException e) {
      throw new H3UtilException(e);
    }
    // init cellSizeMap
    cellSizeMap = new HashMap<>();
    cellSizeMap.put(0, 1257.1651279870703);
    cellSizeMap.put(1, 415.1068893923832);
    cellSizeMap.put(2, 147.84174424387626);
    cellSizeMap.put(3, 54.530253595150185);
    cellSizeMap.put(4, 20.426305676421265);
    cellSizeMap.put(5, 7.693156379723934);
    cellSizeMap.put(6, 2.9039922554770365);
    cellSizeMap.put(7, 1.097050477001042);
    cellSizeMap.put(8, 0.4145696618254594);
    cellSizeMap.put(9, 0.15668127143434735);
    cellSizeMap.put(10, 0.059218394287200805);
    cellSizeMap.put(11, 0.022382219491694798);
    cellSizeMap.put(12, 0.008459648046313144);
    cellSizeMap.put(13, 0.0031974550140400557);
    cellSizeMap.put(14, 0.0012085926447938334);
    cellSizeMap.put(15, 0.0004567448929842399);
  }

  public static LatLng coordindateToLatLng(Coordinate coordinate) {
    return new LatLng(coordinate.getY(), coordinate.getX());
  }

  public static Coordinate cellToCoordinate(long cell) {
    LatLng latLng = h3.cellToLatLng(cell);
    return new Coordinate(latLng.lng, latLng.lat);
  }

  public static long coordinateToCell(Coordinate coordinate, int level) {
    return h3.latLngToCell(coordinate.getY(), coordinate.getX(), level);
  }

  public static List<Long> polygonToCells(Polygon polygon, int level, boolean fullCover) {
    try {
      List<LatLng> shell =
          Arrays.stream(polygon.getExteriorRing().getCoordinates())
              .map(H3Utils::coordindateToLatLng)
              .collect(Collectors.toList());
      List<List<LatLng>> holes = new ArrayList<>();
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        holes.add(
            Arrays.stream(polygon.getInteriorRingN(i).getCoordinates())
                .map(H3Utils::coordindateToLatLng)
                .collect(Collectors.toList()));
      }
      // H3 polyfill only include hexagons with centroid within the polygon, we fix by generating
      // the neighbors for the cells.
      List<Long> internalCells = h3.polygonToCells(shell, holes.isEmpty() ? null : holes, level);
      // if the internal cell is empty, means no cell with centroid within, we'll use centroid to
      // add a seed
      if (internalCells.isEmpty()) {
        internalCells.add(coordinateToCell(polygon.getCentroid().getCoordinate(), level));
      }
      if (fullCover) {
        // H3 polyfill only include hexagons with centroid within the polygon, we add augmentation
        // to guarantee full coverage.
        Set<Long> cells = new HashSet<>();
        // this augment shouldn't be required with cells covering shells, just in case we add them
        internalCells.forEach(cell -> cells.addAll(h3.gridDisk(cell, 1)));
        // further augment by adding the shell and holes cover cells
        cells.addAll(lineStringToCells(polygon.getExteriorRing(), level, true));
        for (int it = 0; it < polygon.getNumInteriorRing(); it++) {
          LineString hole = polygon.getInteriorRingN(it);
          cells.addAll(lineStringToCells(hole, level, true));
        }
        return new ArrayList<>(cells);
      } else {
        return internalCells;
      }
    } catch (H3Exception e) {
      throw new H3UtilException(
          String.format(
              "fail to cover the polygon %s with error %s",
              GeomUtils.getWKT(polygon), e.getMessage()),
          e);
    }
  }

  public static double latLngDistance(double lat1, double lat2, double lon1, double lon2) {

    final int R = 6371; // Radius of the earth

    double latDistance = Math.toRadians(lat2 - lat1);
    double lonDistance = Math.toRadians(lon2 - lon1);
    double a =
        Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
            + Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2)
                * Math.sin(lonDistance / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }

  public static List<Long> approxPathCells(Coordinate sc, Coordinate ec, int level, boolean safe) {
    double approxDistance = latLngDistance(sc.y, ec.y, sc.x, ec.x);
    int itCellNum = Math.max((int) (approxDistance / cellSizeMap.get(level) * 3), 1);
    double xUnit = (ec.getX() - sc.getX()) / itCellNum;
    double yUnit = (ec.getY() - sc.getY()) / itCellNum;
    // keep order of cells
    Set<Long> cells = new LinkedHashSet<>();
    for (int it = 0; it <= itCellNum; ++it) {
      cells.add(
          coordinateToCell(new Coordinate(sc.getX() + xUnit * it, sc.getY() + yUnit * it), level));
    }
    // truncate the duplicate case where 1 cell connect to > 1 neighbor cell.
    List<Long> approxCells = new ArrayList<>(cells);
    // clear cells, put the first cell as start
    cells.clear();
    cells.add(approxCells.get(0));
    Set<Long> closetNeighbors = new HashSet<>(h3.gridDisk(approxCells.get(0), 1));
    List<Long> candidates = new ArrayList<>();
    int it = 1;
    while (it < approxCells.size()) {
      // if it's one of the neighbors of the closet cell,
      long curCell = approxCells.get(it);
      if (closetNeighbors.contains(curCell)) {
        // do 1 step backward check
        candidates.add(curCell);
        it++;
      } else {
        // the curCell is not the neighbor of closest cell. means check for candidates ended.
        long pickedCandidate = 0;
        for (Long candidate : candidates) {
          List<Long> candidateNeighbors = h3.gridDisk(candidate, 1);
          if (candidateNeighbors.contains(curCell)) {
            // find the cell that connect to both cur and closest, add into final list
            pickedCandidate = candidate;
            // if the candidate connect to both cur and next cell, means it's better to pick it
            if (it + 1 < approxCells.size()
                && candidateNeighbors.contains(approxCells.get(it + 1))) {
              break;
            }
          }
        }
        if (pickedCandidate != 0) {
          cells.add(pickedCandidate);
          candidates.clear();
          closetNeighbors = new HashSet<>(h3.gridDisk(pickedCandidate, 1));
        } else if (!safe) {
          // if not need to guarantee safe, randomly pick 1 candidate
          cells.add(candidates.get(0));
          cells.add(curCell);
          closetNeighbors = new HashSet<>(h3.gridDisk(curCell, 1));
          candidates.clear();
          // if not raise error, make sure it always rotate
          it++;
        } else {
          // our estimation shouldn't generate two adjacent cells in list that are not neighbors.
          // because we split the list by the min cell edge length / 3, and the least possible
          // distance between the edges of 2 hexagon
          // with 1 hexagon in between equals to min cell edge length. We'll throw error here if the
          // edge case found and use it for corner case study.
          // no matter what, move the iteration
          throw new H3UtilException(
              String.format(
                  "fail to approximate path between cell %d to %d, generated gap cell %d",
                  coordinateToCell(sc, level), coordinateToCell(ec, level), curCell));
        }
      }
    }
    if (!candidates.isEmpty()) {
      cells.add(candidates.get(0));
    }
    return new ArrayList<>(cells);
  }

  public static List<Long> lineStringToCells(LineString line, int level, boolean fullCover) {
    try {
      Coordinate[] coordinates = line.getCoordinates();
      Set<Long> cells = new LinkedHashSet<>();
      // for each segment, generate coverage separately
      for (int pairE = 1; pairE < coordinates.length; pairE++) {
        Coordinate cs = coordinates[pairE - 1];
        Coordinate ce = coordinates[pairE];
        long cellS = coordinateToCell(cs, level);
        long cellE = coordinateToCell(ce, level);
        List<Long> pathCells;
        try {
          try {
            pathCells = h3.gridPathCells(cellS, cellE);
          } catch (H3Exception e) {
            pathCells = approxPathCells(cs, ce, level, true);
          }
          if (pathCells.size() <= 2 || !fullCover) {
            cells.addAll(pathCells);
          } else {
            // the shortest path from cs to ce not guarantee cover the line, for the missed part, we
            // generate neighbors for the cells to cover.
            // We exclude the cells that only adjacent to 1 cell in the path to remove some
            // redundancy, but this won't remove all redundancy.
            Set<Long> prevNeighbors = new HashSet<>(h3.gridDisk(pathCells.get(0), 1));
            for (int it = 1; it < pathCells.size(); it++) {
              Set<Long> neighbors = new HashSet<>(h3.gridDisk(pathCells.get(it), 1));
              prevNeighbors.retainAll(neighbors);
              cells.addAll(prevNeighbors);
              prevNeighbors = neighbors;
            }
          }
        } catch (H3UtilException e) {
          if (fullCover) {
            cells.addAll(
                polygonToCells(
                    (Polygon)
                        (line.getFactory()
                            .createLineString(new Coordinate[] {cs, ce})
                            .getEnvelope()),
                    level,
                    true));
          } else {
            // if not full cover, we'll throw error, cause for getting the shortest path, we suppose
            // should have no error
            throw e;
          }
        }
      }
      return new ArrayList<>(cells);
    } catch (Exception e) {
      throw new H3UtilException(
          String.format(
              "fail to cover the line %s with error %s", GeomUtils.getWKT(line), e.getMessage()),
          e);
    }
  }
}
