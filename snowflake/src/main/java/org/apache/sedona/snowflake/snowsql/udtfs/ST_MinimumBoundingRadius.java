package org.apache.sedona.snowflake.snowsql.udtfs;

import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(name = "ST_MinimumBoundingRadius", argNames = {"geom"})
public class ST_MinimumBoundingRadius {

    public static final GeometryFactory geometryFactory = new GeometryFactory();

    public static class OutputRow {

        public byte[] center;

        public double radius;

        public OutputRow(byte[] center, double radius) {
            this.center = center;
            this.radius = radius;
        }
    }

    public ST_MinimumBoundingRadius() {
    }

    public Stream<OutputRow> process(byte[] geom) throws ParseException {
        Geometry geometry = GeometrySerde.deserialize(geom);
        MinimumBoundingCircle minimumBoundingCircle = new MinimumBoundingCircle(geometry);
        return Stream.of(new OutputRow(
                GeometrySerde.serialize(
                        geometryFactory.createPoint(minimumBoundingCircle.getCentre())
                ),
                minimumBoundingCircle.getRadius()

        ));
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }
}
