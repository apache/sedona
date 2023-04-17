package org.apache.sedona.common.simplify;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class LineStringSimplifier extends BaseSimplifier {

    public static Geometry simplify(Geometry geom, boolean preserveCollapsed, double epsilon) {
        Coordinate[] simplified = CoordinatesSimplifier.simplifyInPlace(geom.getCoordinates(), epsilon, 2);

        if (simplified.length == 1){
            if (preserveCollapsed) return geometryFactory.createLineString(ArrayUtils.addAll(simplified, simplified));
            else return geometryFactory.createLineString(simplified);
        }
        else if (simplified.length == 2 && !preserveCollapsed){
            if (simplified[0] == simplified[1]) return geometryFactory.createLineString(simplified);
            else return geometryFactory.createLineString(simplified);
        }
        else {
            return geometryFactory.createLineString(simplified);
        }
    }
}
