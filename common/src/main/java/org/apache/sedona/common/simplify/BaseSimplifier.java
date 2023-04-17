package org.apache.sedona.common.simplify;

import org.locationtech.jts.geom.GeometryFactory;

public abstract class BaseSimplifier {
    protected static GeometryFactory geometryFactory = new GeometryFactory();
}
