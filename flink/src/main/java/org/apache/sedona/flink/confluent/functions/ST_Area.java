package org.apache.sedona.flink.confluent.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.flink.confluent.GeometrySerde;
import org.locationtech.jts.geom.Geometry;

class ST_Area extends ScalarFunction {
    @DataTypeHint("Double")
    public Double eval(byte[] o) {
        Geometry geom = GeometrySerde.deserialize(o);
        return org.apache.sedona.common.Functions.area(geom);
    }
}

