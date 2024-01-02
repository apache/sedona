package org.apache.sedona.snowflake.snowsql.udtfs;

import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(name = "ST_Dump", argNames = {"geomCollection"})
public class ST_Dump {

    public static class OutputRow {

        public byte[] geom;

        public OutputRow(byte[] geom) {
            this.geom = geom;
        }
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    public ST_Dump() {}

    public Stream<OutputRow> process(byte[] geomCollection) throws ParseException {
        return Stream.of(GeometrySerde.deserialize2List(geomCollection)).map(g -> new OutputRow(GeometrySerde.serialize(g)));
    }
}
