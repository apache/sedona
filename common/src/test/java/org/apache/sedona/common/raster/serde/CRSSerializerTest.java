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
package org.apache.sedona.common.raster.serde;

import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class CRSSerializerTest {
  @Test
  public void testCRSSerializer() throws FactoryException {
    CoordinateReferenceSystem crs = CRS.decode("EPSG:32607");
    byte[] serializedCRS = CRSSerializer.serialize(crs);
    CoordinateReferenceSystem deserializedCRS = CRSSerializer.deserialize(serializedCRS);
    Assert.assertTrue(CRS.equalsIgnoreMetadata(crs, deserializedCRS));
  }

  @Test
  public void testCRSSerializerWithoutCache() throws FactoryException {
    CoordinateReferenceSystem crs = CRS.decode("EPSG:32607");
    byte[] serializedCRS = CRSSerializer.serialize(crs);
    CRSSerializer.invalidateCache();
    CoordinateReferenceSystem deserializedCRS = CRSSerializer.deserialize(serializedCRS);
    Assert.assertTrue(CRS.equalsIgnoreMetadata(crs, deserializedCRS));
  }
}
