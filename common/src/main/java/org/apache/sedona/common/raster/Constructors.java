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
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.gce.geotiff.GeoTiffReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class Constructors {

    public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            ArcGridReader reader = new ArcGridReader(inputStream);
            return reader.read(null);
        }
    }

    public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            GeoTiffReader geoTiffReader = new GeoTiffReader(inputStream);
            return geoTiffReader.read(null);
        }
    }
}
