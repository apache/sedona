package org.apache.sedona.flink.expressions;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;


public class FunctionsGeoTools {
    public static class ST_Transform extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS)
            throws FactoryException, TransformException {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.FunctionsGeoTools.transform(geom, sourceCRS, targetCRS);
        }

        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS, @DataTypeHint("Boolean") Boolean lenient)
                throws FactoryException, TransformException {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.FunctionsGeoTools.transform(geom, sourceCRS, targetCRS, lenient);
        }
    }
}
