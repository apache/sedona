package org.apache.sedona.common.simplify;

import org.apache.sedona.common.geometryObjects.Circle;
import org.locationtech.jts.geom.Geometry;

public class CircleSimplifier {
    public static Geometry simplify(Circle geom, boolean preserveCollapsed, double epsilon) {
        return geom;
    }
}
