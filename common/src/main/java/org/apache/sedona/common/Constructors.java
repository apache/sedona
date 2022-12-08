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
}
