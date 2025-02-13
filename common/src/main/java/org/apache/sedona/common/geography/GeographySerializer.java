package org.apache.sedona.common.geography;

import org.apache.sedona.common.geometrySerde.GeometrySerializer;

class GeographySerializer {
    public static byte[] serialize(Geography geography) {
        return GeometrySerializer.serialize(geography.getGeometry());
    }
}
