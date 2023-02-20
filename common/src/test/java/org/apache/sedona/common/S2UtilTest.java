/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import org.apache.sedona.common.utils.S2Utils;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class S2UtilTest {
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private Coordinate[] coordArray(double... coordValues) {
        Coordinate[] coords = new Coordinate[(int)(coordValues.length / 2)];
        for (int i = 0; i < coordValues.length; i += 2) {
            coords[(int)(i / 2)] = new Coordinate(coordValues[i], coordValues[i+1]);
        }
        return coords;
    }

    @Test
    public void toS2Point() throws ParseException {
        Coordinate jtsCoord = new Coordinate(1, 2);
        S2Point s2Point = S2Utils.toS2Point(jtsCoord);
        S2LatLng latLng = new S2LatLng(s2Point);
        assertEquals(Math.round(latLng.lngDegrees()), 1);
        assertEquals(Math.round(latLng.latDegrees()), 2);
    }

    @Test
    public void coverPolygon() throws ParseException {
        Polygon polygon = (Polygon) new WKTReader().read("POLYGON ((0.5 0.5, 5 0, 6 3, 5 5, 0 5, 0.5 0.5))");
        List<S2CellId> cellIds = S2Utils.s2RegionToCellIDs(S2Utils.toS2Polygon(polygon), 1, 5, Integer.MAX_VALUE);
        assertEquals(5, cellIds.size());
        assertEquals(cellIds.stream().max(Comparator.comparingLong(S2CellId::level)).get().level(), 5);
    }

    @Test
    public void coverPolygonWithHole() throws ParseException {
        Polygon polygon = (Polygon) new WKTReader().read("POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 1,4 3,1.5 1))");
        Polygon hole = (Polygon) new WKTReader().read("POLYGON((1.5 1,4 1,4 3,1.5 1))");
        List<S2CellId> cellIds = S2Utils.roundCellsToSameLevel(S2Utils.s2RegionToCellIDs(S2Utils.toS2Polygon(polygon), 1, 8, Integer.MAX_VALUE-1), 8);
        S2CellId holeCentroidCell = S2Utils.coordinateToCellID(hole.getCentroid().getCoordinate(), 8);
        S2CellId holeFirstVertexCell = S2Utils.coordinateToCellID(hole.getCoordinates()[0], 8);
        assertEquals(8, cellIds.stream().max(Comparator.comparingLong(S2CellId::level)).get().level());
        assert(!cellIds.contains(holeCentroidCell));
        assert(cellIds.contains(holeFirstVertexCell));

    }

    @Test
    public void coverLineString() throws ParseException {
        LineString line = (LineString) new WKTReader().read("LINESTRING (1.5 2.45, 3.21 4)");
        List<S2CellId> cellIds = S2Utils.s2RegionToCellIDs(S2Utils.toS2PolyLine(line), 1, 8, Integer.MAX_VALUE);
        assertEquals(12, cellIds.size());
        assertEquals(cellIds.stream().max(Comparator.comparingLong(S2CellId::level)).get().level(), 8);
    }

    @Test
    public void coverLinearLoop() throws ParseException {
        LineString line = GEOMETRY_FACTORY.createLineString(new WKTReader().read("LINESTRING (1.5 2.45, 3.21 4, 5 2, 1.5 2.45)").getCoordinates());
        List<S2CellId> cellIds = S2Utils.s2RegionToCellIDs(S2Utils.toS2PolyLine(line), 1, 8, Integer.MAX_VALUE);
        assertEquals(31, cellIds.size());
        assertEquals(cellIds.stream().max(Comparator.comparingLong(S2CellId::level)).get().level(), 8);
    }

    @Test
    public void toS2Loop() throws ParseException {
        LinearRing ringCW = GEOMETRY_FACTORY.createLinearRing(new WKTReader().read("LINESTRING (1.5 2.45, 3.21 4, 5 2, 1.5 2.45)").getCoordinates());
        LinearRing ringCCW = GEOMETRY_FACTORY.createLinearRing(new WKTReader().read("LINESTRING (1.5 2.45, 5 2, 3.21 4, 1.5 2.45)").getCoordinates());
        assert(ringCCW != ringCW);
        S2Loop s2Loop = S2Utils.toS2Loop(ringCW);
        DecimalFormat df = new DecimalFormat("#.##");
        LinearRing reversedRing = GEOMETRY_FACTORY.createLinearRing(
                s2Loop.vertices().stream().map(S2LatLng::new).map(l -> new Coordinate(Double.parseDouble(df.format(l.lngDegrees())), Double.parseDouble(df.format(l.latDegrees())))).collect(Collectors.toList()).toArray(new Coordinate[4])
        );
        assertEquals(ringCCW, reversedRing);
    }
}
