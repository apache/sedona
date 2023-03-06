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
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.core.spatialPartitioning.PartitioningUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Objects;

public class Predicates {
    public static class ST_Intersects extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_Intersects(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_Intersects() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.intersects(geom2);
        }

        /**
         * Check spatial relation with duplicates removal
         * @param key
         * @param o1
         * @param o2
         * @return True if intersecting, false otherwise
         */
        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint("INT") Integer key, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Objects.requireNonNull(grids, "This predicate has to be initialized by a partitioner.");
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            HalfOpenRectangle halfOpenRectangle = new HalfOpenRectangle(grids.get(key));
            return !GeomUtils.isDuplicate(geom1, geom2, halfOpenRectangle) && geom1.intersects(geom2);
        }
    }

    public static class ST_Contains extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_Contains(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_Contains() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.contains(geom2);
        }

        /**
         * Check spatial relation with duplicates removal
         * @param key
         * @param o1
         * @param o2
         * @return True if o1 contains o2, false otherwise
         */
        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint("INT") Integer key, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Objects.requireNonNull(grids, "This predicate has to be initialized by a partitioner.");
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            HalfOpenRectangle halfOpenRectangle = new HalfOpenRectangle(grids.get(key));
            return !GeomUtils.isDuplicate(geom1, geom2, halfOpenRectangle) && geom1.contains(geom2);
        }
    }

    public static class ST_Within extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_Within(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_Within() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.within(geom2);
        }

        /**
         * Check spatial relation with duplicates removal
         * @param key
         * @param o1
         * @param o2
         * @return True if o1 is within o2, false otherwise
         */
        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint("INT") Integer key, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Objects.requireNonNull(grids, "This predicate has to be initialized by a partitioner.");
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            HalfOpenRectangle halfOpenRectangle = new HalfOpenRectangle(grids.get(key));
            return !GeomUtils.isDuplicate(geom1, geom2, halfOpenRectangle) && geom1.within(geom2);
        }
    }

    public static class ST_Covers extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_Covers(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_Covers() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.covers(geom2);
        }

        /**
         * Check spatial relation with duplicates removal
         * @param key
         * @param o1
         * @param o2
         * @return True if o1 covers o2, false otherwise
         */
        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint("INT") Integer key, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Objects.requireNonNull(grids, "This predicate has to be initialized by a partitioner.");
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            HalfOpenRectangle halfOpenRectangle = new HalfOpenRectangle(grids.get(key));
            return !GeomUtils.isDuplicate(geom1, geom2, halfOpenRectangle) && geom1.covers(geom2);
        }
    }

    public static class ST_CoveredBy extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_CoveredBy(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_CoveredBy() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.coveredBy(geom2);
        }

        /**
         * Check spatial relation with duplicates removal
         * @param key
         * @param o1
         * @param o2
         * @return True if o1 is covered by o2, false otherwise
         */
        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint("INT") Integer key, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Objects.requireNonNull(grids, "This predicate has to be initialized by a partitioner.");
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            HalfOpenRectangle halfOpenRectangle = new HalfOpenRectangle(grids.get(key));
            return !GeomUtils.isDuplicate(geom1, geom2, halfOpenRectangle) && geom1.coveredBy(geom2);
        }
    }

    public static class ST_Disjoint extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_Disjoint(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_Disjoint() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.disjoint(geom2);
        }
    }

    public static class ST_OrderingEquals extends ScalarFunction {
        private List<Envelope> grids;

        /**
         * Constructor for duplicate removal
         */
        public ST_OrderingEquals(PartitioningUtils partitioner) {
            grids = partitioner.fetchLeafZones();
        }

        /**
         * Constructor for relation checking without duplicate removal
         */
        public ST_OrderingEquals() {
        }

        @DataTypeHint("Boolean")
        public Boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return geom1.equalsExact(geom2);
        }
    }
}
