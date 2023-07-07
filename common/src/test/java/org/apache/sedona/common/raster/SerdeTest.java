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
package org.apache.sedona.common.raster;

import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.geometry.DirectPosition;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class SerdeTest extends RasterTestBase {

    @Test
    public void testRoundtripSerdeSingelbandRaster() throws IOException, ClassNotFoundException {
        byte[] bytes = Serde.serialize(oneBandRaster);
        GridCoverage2D raster = Serde.deserialize(bytes);
        assertNotNull(raster);
        assertSameCoverage(oneBandRaster, raster);
        bytes = Serde.serialize(raster);
        raster = Serde.deserialize(bytes);
        assertSameCoverage(oneBandRaster, raster);
    }

    @Test
    public void testRoundtripSerdeMultibandRaster() throws IOException, ClassNotFoundException {
        byte[] bytes = Serde.serialize(this.multiBandRaster);
        GridCoverage2D raster = Serde.deserialize(bytes);
        assertNotNull(raster);
        assertSameCoverage(multiBandRaster, raster);
        bytes = Serde.serialize(raster);
        raster = Serde.deserialize(bytes);
        assertSameCoverage(multiBandRaster, raster);
    }

    private void assertSameCoverage(GridCoverage2D expected, GridCoverage2D actual) {
        Assert.assertEquals(expected.getNumSampleDimensions(), actual.getNumSampleDimensions());
        Envelope expectedEnvelope = expected.getEnvelope();
        Envelope actualEnvelope = actual.getEnvelope();
        assertSameEnvelope(expectedEnvelope, actualEnvelope, 1e-6);
        CoordinateReferenceSystem expectedCrs = expected.getCoordinateReferenceSystem();
        CoordinateReferenceSystem actualCrs = actual.getCoordinateReferenceSystem();
        Assert.assertTrue(CRS.equalsIgnoreMetadata(expectedCrs, actualCrs));
        assertSameValues(expected, actual, 10);
    }

    private void assertSameEnvelope(Envelope expected, Envelope actual, double epsilon) {
        Assert.assertEquals(expected.getMinimum(0), actual.getMinimum(0), epsilon);
        Assert.assertEquals(expected.getMinimum(1), actual.getMinimum(1), epsilon);
        Assert.assertEquals(expected.getMaximum(0), actual.getMaximum(0), epsilon);
        Assert.assertEquals(expected.getMaximum(1), actual.getMaximum(1), epsilon);
    }

    private void assertSameValues(GridCoverage2D expected, GridCoverage2D actual, int density) {
        Envelope expectedEnvelope = expected.getEnvelope();
        double x0 = expectedEnvelope.getMinimum(0);
        double y0 = expectedEnvelope.getMinimum(1);
        double xStep = (expectedEnvelope.getMaximum(0) - x0) / density;
        double yStep = (expectedEnvelope.getMaximum(1) - y0) / density;
        double[] expectedValues = new double[expected.getNumSampleDimensions()];
        double[] actualValues = new double[expected.getNumSampleDimensions()];
        int sampledPoints = 0;
        for (int i = 0; i < density; i++) {
            for (int j = 0; j < density; j++) {
                double x = x0 + j * xStep;
                double y = y0 + i * yStep;
                DirectPosition position = new DirectPosition2D(x, y);
                try {
                    GridCoordinates2D gridPosition = expected.getGridGeometry().worldToGrid(position);
                    if (Double.isNaN(gridPosition.getX()) || Double.isNaN(gridPosition.getY())) {
                        // This position is outside the coverage
                        continue;
                    }
                    expected.evaluate(position, expectedValues);
                    actual.evaluate(position, actualValues);
                    Assert.assertEquals(expectedValues.length, actualValues.length);
                    for (int k = 0; k < expectedValues.length; k++) {
                        Assert.assertEquals(expectedValues[k], actualValues[k], 1e-6);
                    }
                    sampledPoints += 1;
                } catch (TransformException e) {
                    throw new RuntimeException("Failed to convert world coordinate to grid coordinate", e);
                }
            }
        }
        Assert.assertTrue(sampledPoints > density * density / 2);
    }
}
