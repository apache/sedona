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

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.utils.FormatUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class Constructors {

    public static Geometry geomFromWKT(String wkt, int srid) throws ParseException {
        if (wkt == null) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }

    public static Geometry mLineFromText(String wkt, int srid) throws ParseException {
        if (wkt == null || !wkt.startsWith("MULTILINESTRING")) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }

    public static Geometry mPolyFromText(String wkt, int srid) throws ParseException {
        if (wkt == null || !wkt.startsWith("MULTIPOLYGON")) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }


    /**
     * Creates a point from the given coordinate.
     * ST_Point in Sedona Spark API took an optional z value before v1.4.0.
     * This was removed to avoid confusion with other GIS implementations where the optional third argument is srid.
     *
     * A future version of Sedona will add a srid parameter once enough users have upgraded and hence are forced
     * to use ST_PointZ for 3D points.
     *
     * @param x the x value
     * @param y the y value
     * @return The point geometry
     */
    public static Geometry point(double x, double y) {
        // See srid parameter discussion in https://issues.apache.org/jira/browse/SEDONA-234
        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    /**
     * Creates a point from the given coordinate.
     *
     * @param x the x value
     * @param y the y value
     * @param z the z value
     * @param srid Set to 0 if unknown
     * @return The point geometry
     */
    public static Geometry pointZ(double x, double y, double z, int srid) {
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return geometryFactory.createPoint(new Coordinate(x, y, z));
    }

    public static Geometry geomFromText(String geomString, String geomFormat, GeometryType geometryType) {
        FileDataSplitter fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat);
        FormatUtils<Geometry> formatMapper = new FormatUtils<>(fileDataSplitter, false, geometryType);
        try {
            return formatMapper.readGeometry(geomString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Geometry geomFromText(String geomString, FileDataSplitter fileDataSplitter) {
        FormatUtils<Geometry> formatMapper = new FormatUtils<>(fileDataSplitter, false);
        try {
            return formatMapper.readGeometry(geomString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
