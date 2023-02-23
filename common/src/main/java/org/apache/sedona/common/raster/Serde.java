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

import javax.media.jai.remote.SerializableRenderedImage;
import java.awt.image.RenderedImage;
import java.io.*;

public class Serde {

    public static byte[] serialize(GridCoverage2D raster) throws IOException {
        // GridCoverage2D created by GridCoverage2DReaders contain references that are not serializable.
        // Wrap the RenderedImage in SerializableRenderedImage to make it serializable.
        if (!(raster.getRenderedImage() instanceof SerializableRenderedImage)) {
            RenderedImage renderedImage = new SerializableRenderedImage(
                    raster.getRenderedImage(),
                    false,
                    null,
                    "gzip",
                    null,
                    null
            );
            raster = new GridCoverageFactory().create(
                    raster.getName(),
                    renderedImage,
                    raster.getGridGeometry(),
                    raster.getSampleDimensions(),
                    null,
                    raster.getProperties()
            );
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(raster);
                return bos.toByteArray();
            }
        }
    }

    public static GridCoverage2D deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream ois = new ObjectInputStream(bis)) {
                return (GridCoverage2D) ois.readObject();
            }
        }
    }
}
