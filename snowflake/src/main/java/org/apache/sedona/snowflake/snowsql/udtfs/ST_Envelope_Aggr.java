package org.apache.sedona.snowflake.snowsql.udtfs;

import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(
        name = "ST_Envelope_Aggr",
        argNames = {"geom"}
)
public class ST_Envelope_Aggr {

    public static final GeometryFactory geometryFactory = new GeometryFactory();

    Envelope buffer = null;

    public static class OutputRow {

        public byte[] envelope;

        public OutputRow(byte[] envelopePolygon) {
            this.envelope = envelopePolygon;
        }
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    public ST_Envelope_Aggr() {
    }

    public Stream<OutputRow> process(byte[] geom) throws ParseException {
        Geometry geometry = GeometrySerde.deserialize(geom);
        if (buffer == null) {
            buffer = geometry.getEnvelopeInternal();
        } else {
            buffer.expandToInclude(geometry.getEnvelopeInternal());
        }
        return Stream.empty();
    }

    public Stream<OutputRow> endPartition() {
        // Returns the value we initialized in the constructor.
        Polygon poly = geometryFactory.createPolygon(geometryFactory.createLinearRing(new Coordinate[] {
                new Coordinate(buffer.getMinX(), buffer.getMinY()),
                new Coordinate(buffer.getMinX(), buffer.getMaxY()),
                new Coordinate(buffer.getMaxX(), buffer.getMaxY()),
                new Coordinate(buffer.getMaxX(), buffer.getMinY()),
                new Coordinate(buffer.getMinX(), buffer.getMinY())
        }));
        return Stream.of(new OutputRow(GeometrySerde.serialize(poly)));
    }
}
