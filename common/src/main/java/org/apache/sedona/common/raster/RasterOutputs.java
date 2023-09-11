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

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.gce.arcgrid.ArcGridWriteParams;
import org.geotools.gce.arcgrid.ArcGridWriter;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.opengis.coverage.grid.GridCoverageWriter;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;

import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;

public class RasterOutputs
{
    public static byte[] asGeoTiff(GridCoverage2D raster, String compressionType, double compressionQuality) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GridCoverageWriter writer;
        try {
            writer = new GeoTiffWriter(out);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        ParameterValueGroup defaultParams = writer.getFormat().getWriteParameters();
        if (compressionType != null && compressionQuality >= 0 && compressionQuality <= 1) {
            GeoTiffWriteParams params = new GeoTiffWriteParams();
            params.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            // Available compression types: None, PackBits, Deflate, Huffman, LZW and JPEG
            params.setCompressionType(compressionType);
            // Should be a value between 0 and 1
            // 0 means max compression, 1 means no compression
            params.setCompressionQuality((float) compressionQuality);
            defaultParams.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString()).setValue(params);
        }
        GeneralParameterValue[] wps = defaultParams.values().toArray(new GeneralParameterValue[0]);
        try {
            writer.write(raster, wps);
            writer.dispose();
            out.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    public static byte[] asGeoTiff(GridCoverage2D raster) {
        return asGeoTiff(raster, null, -1);
    }

    /**
    * Creates a GeoTiff file with the provided raster. Primarily used for testing.
     * @param bytes The bytes to be stored on a disk file
     * @param filePath The path where the .tiff should be stored.
     *
     * @return true if file is created, otherwise throws IOException
     * @throws IOException
     * */
    public static boolean writeToDiskFile(byte[] bytes, String filePath) {
        File outputFile = new File(filePath);
        try (FileOutputStream outputStream = new FileOutputStream(outputFile)){
            outputStream.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public static byte[] asArcGrid(GridCoverage2D raster, int sourceBand) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GridCoverageWriter writer;
        try {
            writer = new ArcGridWriter(out);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        ParameterValueGroup defaultParams = writer.getFormat().getWriteParameters();
        if (sourceBand >= 0) {
            ArcGridWriteParams params = new ArcGridWriteParams();
            params.setSourceBands(new int[]{sourceBand});
            defaultParams.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString()).setValue(params);
        }
        GeneralParameterValue[] wps = defaultParams.values().toArray(new GeneralParameterValue[0]);
        try {
            writer.write(raster, wps);
            writer.dispose();
            out.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    public static byte[] asArcGrid(GridCoverage2D raster) {
        return asArcGrid(raster, -1);
    }

    public static String asBase64(GridCoverage2D raster) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RenderedImage renderedImage = raster.getRenderedImage();
        ImageIO.write(renderedImage, "png", out);
        return Base64.getEncoder().encodeToString(out.toByteArray());
    }
}
