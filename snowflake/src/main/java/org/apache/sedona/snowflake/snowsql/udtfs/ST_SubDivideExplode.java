package org.apache.sedona.snowflake.snowsql.udtfs;

import org.apache.sedona.common.Functions;
import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(name = "ST_SubDivideExplode", argNames = {"geom", "maxVertices"})
public class ST_SubDivideExplode {

    public static final GeometryFactory geometryFactory = new GeometryFactory();

    public static class OutputRow {

        public byte[] geom;

        public OutputRow(byte[] geom) {
            this.geom = geom;
        }
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    public ST_SubDivideExplode() {
    }

    public Stream<OutputRow> process(byte[] geometry, int maxVertices) throws ParseException {
        Geometry[] geometries = Functions.subDivide(
                GeometrySerde.deserialize(geometry),
                maxVertices
        );
        return Stream.of(geometries).map(g -> new OutputRow(GeometrySerde.serialize(g)));
    }
}
