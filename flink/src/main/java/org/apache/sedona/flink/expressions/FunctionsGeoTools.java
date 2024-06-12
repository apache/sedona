/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class FunctionsGeoTools {
  public static class ST_Transform extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o,
        @DataTypeHint("String") String targetCRS)
        throws FactoryException, TransformException {
      Geometry geom = (Geometry) o;
      return org.apache.sedona.common.FunctionsGeoTools.transform(geom, targetCRS);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o,
        @DataTypeHint("String") String sourceCRS,
        @DataTypeHint("String") String targetCRS)
        throws FactoryException, TransformException {
      Geometry geom = (Geometry) o;
      return org.apache.sedona.common.FunctionsGeoTools.transform(geom, sourceCRS, targetCRS);
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o,
        @DataTypeHint("String") String sourceCRS,
        @DataTypeHint("String") String targetCRS,
        @DataTypeHint("Boolean") Boolean lenient)
        throws FactoryException, TransformException {
      Geometry geom = (Geometry) o;
      return org.apache.sedona.common.FunctionsGeoTools.transform(
          geom, sourceCRS, targetCRS, lenient);
    }
  }
}
