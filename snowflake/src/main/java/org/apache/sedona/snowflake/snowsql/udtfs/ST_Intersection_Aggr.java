package org.apache.sedona.snowflake.snowsql.udtfs;

import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(name = "ST_InterSection_Aggr", argNames = {"geom"})
public class ST_Intersection_Aggr {
    Geometry buffer = null;

    public static class OutputRow {

        public byte[] intersected;

        public OutputRow(byte[] intersected) {
            this.intersected = intersected;
        }
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    public ST_Intersection_Aggr() {
    }

    public Stream<OutputRow> process(byte[] geom) throws ParseException {
        Geometry geometry = GeometrySerde.deserialize(geom);
        if (buffer == null) {
            buffer = geometry;
        } else if (!buffer.equalsExact(geometry)) {
            buffer = buffer.intersection(geometry);
        }
        return Stream.empty();
    }

    public Stream<OutputRow> endPartition() {
        // Returns the value we initialized in the constructor.
        return Stream.of(new OutputRow(GeometrySerde.serialize(buffer)));
    }
}
