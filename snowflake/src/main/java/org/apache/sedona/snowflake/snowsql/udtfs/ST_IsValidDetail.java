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

import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.ValidDetail;
import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.io.ParseException;

import java.util.stream.Stream;

@UDTFAnnotations.TabularFunc(name = "ST_IsValidDetail", argNames = {"geom", "flag"})
public class ST_IsValidDetail {

    public static class OutputRow {
        public final boolean valid;
        public final String reason;
        public final byte[] location;

        public OutputRow(ValidDetail validDetail) {
            this.valid = validDetail.valid;
            this.reason = validDetail.reason;
            this.location = GeometrySerde.serialize(validDetail.location);
        }
    }

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    public ST_IsValidDetail() {
    }

    public Stream<OutputRow> process(byte[] geometry, Integer flag) throws ParseException {
        ValidDetail validDetail = Functions.isValidDetail(
                GeometrySerde.deserialize(geometry),
                flag
        );

        return Stream.of(new OutputRow(validDetail));
    }
}
