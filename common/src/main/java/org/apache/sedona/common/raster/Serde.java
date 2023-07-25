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

import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedImageAdapter;
import javax.media.jai.remote.SerializableRenderedImage;
import java.awt.image.RenderedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;

public class Serde {

    static final Field field;
    static {
        try {
            field = GridCoverage2D.class.getDeclaredField("serializedImage");
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(GridCoverage2D raster) throws IOException {
        // GridCoverage2D created by GridCoverage2DReaders contain references that are not serializable.
        // Wrap the RenderedImage in DeepCopiedRenderedImage to make it serializable.
        RenderedImage deepCopiedRenderedImage = null;
        RenderedImage renderedImage = raster.getRenderedImage();
        while (renderedImage instanceof RenderedImageAdapter) {
            renderedImage = ((RenderedImageAdapter) renderedImage).getWrappedImage();
        }
        if (renderedImage instanceof DeepCopiedRenderedImage) {
            deepCopiedRenderedImage = renderedImage;
        } else {
            deepCopiedRenderedImage = new DeepCopiedRenderedImage(renderedImage);
        }
        raster = new GridCoverageFactory().create(
                raster.getName(),
                deepCopiedRenderedImage,
                raster.getGridGeometry(),
                raster.getSampleDimensions(),
                null,
                raster.getProperties());

        // Set the serializedImage so that GridCoverage2D will serialize the DeepCopiedRenderedImage object
        // we created above, rather than creating a SerializedRenderedImage and serialize it. The whole point
        // of DeepCopiedRenderedImage of getting rid of SerializedRenderedImage, which is problematic.
        try {
            field.set(raster, deepCopiedRenderedImage);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
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
