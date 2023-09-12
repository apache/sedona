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

import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.opengis.referencing.operation.MathTransform;

import javax.media.jai.RenderedImageAdapter;
import java.awt.image.RenderedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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

    private static class SerializableState implements Serializable {
        public CharSequence name;

        // The following three components are used to construct a GridGeometry2D object.
        // We serialize CRS separately because the default serializer is pretty slow, we use a
        // cached serializer to speed up the serialization and reuse CRS on deserialization.
        public GridEnvelope2D gridEnvelope2D;
        public MathTransform gridToCRS;
        public byte[] serializedCRS;

        public GridSampleDimension[] bands;
        public DeepCopiedRenderedImage image;

        public GridCoverage2D restore() {
            GridGeometry2D gridGeometry = new GridGeometry2D(gridEnvelope2D, gridToCRS, CRSSerializer.deserialize(serializedCRS));
            return new GridCoverageFactory().create(name, image, gridGeometry, bands, null, null);
        }
    }

    public static byte[] serialize(GridCoverage2D raster) throws IOException {
        // GridCoverage2D created by GridCoverage2DReaders contain references that are not serializable.
        // Wrap the RenderedImage in DeepCopiedRenderedImage to make it serializable.
        DeepCopiedRenderedImage deepCopiedRenderedImage = null;
        RenderedImage renderedImage = raster.getRenderedImage();
        while (renderedImage instanceof RenderedImageAdapter) {
            renderedImage = ((RenderedImageAdapter) renderedImage).getWrappedImage();
        }
        if (renderedImage instanceof DeepCopiedRenderedImage) {
            deepCopiedRenderedImage = (DeepCopiedRenderedImage) renderedImage;
        } else {
            deepCopiedRenderedImage = new DeepCopiedRenderedImage(renderedImage);
        }

        SerializableState state = new SerializableState();
        GridGeometry2D gridGeometry = raster.getGridGeometry();
        state.name = raster.getName();
        state.gridEnvelope2D = gridGeometry.getGridRange2D();
        state.gridToCRS = gridGeometry.getGridToCRS2D();
        state.serializedCRS = CRSSerializer.serialize(gridGeometry.getCoordinateReferenceSystem());
        state.bands = raster.getSampleDimensions();
        state.image = deepCopiedRenderedImage;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(state);
                return bos.toByteArray();
            }
        }
    }

    public static GridCoverage2D deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream ois = new ObjectInputStream(bis)) {
                SerializableState state = (SerializableState) ois.readObject();
                return state.restore();
            }
        }
    }
}
