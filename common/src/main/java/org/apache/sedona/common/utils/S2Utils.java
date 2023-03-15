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

import com.google.common.geometry.*;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;

import javax.swing.*;
import java.util.*;
import java.util.stream.Collectors;

public class S2Utils {

    /**
     * @param coord Coordinate: convert a jts coordinate to a S2Point
     * @return
     */
    public static S2Point toS2Point(Coordinate coord) {
        return S2LatLng.fromDegrees(coord.y, coord.x).toPoint();
    }

    public static List<S2Point> toS2Points(Coordinate[] coords) {
        return Arrays.stream(coords).map(S2Utils::toS2Point).collect(Collectors.toList());
    }

    /**
     * @param line
     * @return
     */
    public static S2Polyline toS2PolyLine(LineString line) {
        return new S2Polyline(toS2Points(line.getCoordinates()));
    }

    public static S2Loop toS2Loop(LinearRing ring) {
        return new S2Loop(
                Orientation.isCCW(ring.getCoordinates()) ? toS2Points(ring.getCoordinates()) : toS2Points(ring.reverse().getCoordinates())
        );
    }

    public static S2Polygon toS2Polygon(Polygon polygon) {
        List<LinearRing> rings = new ArrayList<>();
        rings.add(polygon.getExteriorRing());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++){
            rings.add(polygon.getInteriorRingN(i));
        }
        List<S2Loop> s2Loops = rings.stream().map(
                S2Utils::toS2Loop
        ).collect(Collectors.toList());
        return new S2Polygon(s2Loops);
    }

    public static List<S2CellId> s2RegionToCellIDs(S2Region region, int minLevel, int maxLevel, int maxNum) {
        S2RegionCoverer.Builder coverBuilder = S2RegionCoverer.builder();
        coverBuilder.setMinLevel(minLevel);
        coverBuilder.setMaxLevel(maxLevel);
        coverBuilder.setMaxCells(maxNum);
        return coverBuilder.build().getCovering(region).cellIds();
    }

    public static S2CellId coordinateToCellID(Coordinate coordinate, int level) {
        return S2CellId.fromPoint(S2Utils.toS2Point(coordinate)).parent(level);
    }

    public static List<S2CellId> roundCellsToSameLevel(List<S2CellId> cellIDs, int level) {
        Set<Long> results = new HashSet<>();
        for (S2CellId cellID : cellIDs) {
            if (cellID.level() > level) {
                results.add(cellID.parent(level).id());
            } else if(cellID.level() < level) {
                for (S2CellId c = cellID.childBegin(level); !c.equals(cellID.childEnd(level)); c = c.next()) {
                    results.add(c.id());
                }
            } else {
                results.add(cellID.id());
            }
        }
        return results.stream().map(S2CellId::new).collect(Collectors.toList());
    }

    public static Polygon toJTSPolygon(S2CellId cellId) {
        S2LatLngRect bound = new S2Cell(cellId).getRectBound();
        Coordinate[] coords = new Coordinate[5];
        int[] iters = new int[] {0, 1, 2, 3, 0};
        for (int i = 0;i < 5; i++) {
            coords[i] = new Coordinate(bound.getVertex(iters[i]).lngDegrees(), bound.getVertex(iters[i]).latDegrees());
        }
        return new GeometryFactory().createPolygon(coords);
    }

    public static S2Region toS2Region(Geometry geom) throws IllegalArgumentException {
        if (geom instanceof Polygon) {
            return S2Utils.toS2Polygon((Polygon) geom);
        } else if (geom instanceof LineString) {
            return S2Utils.toS2PolyLine((LineString) geom);
        }
        throw new IllegalArgumentException(
                "only object of Polygon, LinearRing, LineString type can be converted to S2Region"
        );
    }
}
