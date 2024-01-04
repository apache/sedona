/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
