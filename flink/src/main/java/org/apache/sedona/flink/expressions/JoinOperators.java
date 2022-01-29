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
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.sedona.core.spatialPartitioning.PartitioningUtils;
import org.locationtech.jts.geom.Geometry;

import java.util.Iterator;
import java.util.Set;

public class JoinOperators {

    @FunctionHint(output = @DataTypeHint("ROW<_joinKey INT NOT NULL>"))
    public static class ST_Keys extends TableFunction {
        PartitioningUtils partitioner;
        public ST_Keys(PartitioningUtils partitioner){
            this.partitioner = partitioner;
        }
        public void eval(@DataTypeHint("RAW") Object o) {
            Geometry geom = (Geometry) o;
            Set<Integer> keys = partitioner.getKeys(geom);
            Iterator<Integer> it = keys.iterator();
            while (it.hasNext()) {
                collect(Row.of(it.next()));
            }
        }
    }

    public static class ST_Key1 extends ScalarFunction {
        PartitioningUtils partitioner;
        public ST_Key1(PartitioningUtils partitioner){
            this.partitioner = partitioner;
        }
        public Integer eval(@DataTypeHint("RAW") Object o) {
            Geometry geom = (Geometry) o;
            Set<Integer> keys = partitioner.getKeys(geom);
            return keys.iterator().next();
        }
    }
}
