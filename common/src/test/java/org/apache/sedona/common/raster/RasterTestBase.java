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
package org.apache.sedona.common.raster;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.referencing.FactoryException;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RasterTestBase {
    String arc = "NCOLS 2\nNROWS 2\nXLLCORNER 378922\nYLLCORNER 4072345\nCELLSIZE 30\nNODATA_VALUE 0\n0 1 2 3\n";
    GridCoverage2D oneBandRaster;
    GridCoverage2D multiBandRaster;
    byte[] geoTiff;

    @Before
    public void setup() throws IOException {
        oneBandRaster = RasterConstructors.fromArcInfoAsciiGrid(arc.getBytes(StandardCharsets.UTF_8));
        multiBandRaster = createMultibandRaster();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        new GeoTiffWriter(bos).write(multiBandRaster, new GeneralParameterValue[]{});
        geoTiff = bos.toByteArray();
    }

    GridCoverage2D createEmptyRaster(int numBands)
            throws FactoryException
    {
        int widthInPixel = 4;
        int heightInPixel = 5;
        double upperLeftX = 101;
        double upperLeftY = 102;
        double cellSize = 2;
        return RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, cellSize);
    }
    GridCoverage2D createMultibandRaster() throws IOException {
        GridCoverageFactory factory = new GridCoverageFactory();
        BufferedImage image = new BufferedImage(10, 10, BufferedImage.TYPE_INT_ARGB);
        for (int i = 0; i < image.getHeight(); i++) {
            for (int j = 0; j < image.getWidth(); j++) {
                int color = i + j;
                image.setRGB(i, j, new Color(color, color, color).getRGB());
            }
        }
        return factory.create("test", image, new Envelope2D(DefaultGeographicCRS.WGS84, 0, 0, 10, 10));
    }
}
