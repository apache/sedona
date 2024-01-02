package org.apache.sedona.snowflake.snowsql.udtfs;

import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(name = "ST_Union_Aggr", argNames = {"geom"})
public class ST_Union_Aggr {
    public static final GeometryFactory geometryFactory = new GeometryFactory();

    Geometry buffer = null;

    public static class OutputRow {

        public byte[] unioned;

        public OutputRow(byte[] unioned) {
            this.unioned = unioned;
        }
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    public ST_Union_Aggr() {
    }

    public Stream<OutputRow> process(byte[] geom) throws ParseException {
        Geometry geometry = GeometrySerde.deserialize(geom);
        if (buffer == null) {
            buffer = geometry;
        } else if (!buffer.equalsExact(geometry)) {
            buffer = buffer.union(geometry);
        }
        return Stream.empty();
    }

    public Stream<OutputRow> endPartition() {
        // Returns the value we initialized in the constructor.
        return Stream.of(new OutputRow(GeometrySerde.serialize(buffer)));
    }
}
