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

import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.raster.inputstream.ByteArrayImageInputStream;
import org.apache.sedona.common.raster.netcdf.NetCdfReader;
import org.apache.sedona.common.utils.ImageUtils;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.vector.VectorToRasterProcess;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class RasterConstructors {
  public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
    ArcGridReader reader =
        new ArcGridReader(
            new ByteArrayImageInputStream(bytes),
            new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
    return reader.read(null);
  }

  public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
    GeoTiffReader geoTiffReader =
        new GeoTiffReader(
            new ByteArrayImageInputStream(bytes),
            new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
    return geoTiffReader.read(null);
  }

  public static GridCoverage2D fromNetCDF(
      byte[] bytes, String variableName, String lonDimensionName, String latDimensionName)
      throws IOException, FactoryException {
    NetcdfFile netcdfFile = openNetCdfBytes(bytes);
    return NetCdfReader.getRaster(netcdfFile, variableName, latDimensionName, lonDimensionName);
  }

  public static GridCoverage2D fromNetCDF(byte[] bytes, String recordVariableName)
      throws IOException, FactoryException {
    NetcdfFile netcdfFile = openNetCdfBytes(bytes);
    return NetCdfReader.getRaster(netcdfFile, recordVariableName);
  }

  public static String getRecordInfo(byte[] bytes) throws IOException {
    NetcdfFile netcdfFile = openNetCdfBytes(bytes);
    return NetCdfReader.getRecordInfo(netcdfFile);
  }

  private static NetcdfFile openNetCdfBytes(byte[] bytes) throws IOException {
    return NetcdfFiles.openInMemory("", bytes);
  }

  private static void printWritableRasterWithCoordinates(WritableRaster raster) {

    int width = raster.getWidth();
    int height = raster.getHeight();

    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        // Get the value of the first band at this pixel
        double value = raster.getSampleDouble(x, y, 0);

        // Print the coordinates and value
        System.out.printf("(%d,%d):%6.2f  ", x, y, value);
      }
      System.out.println(); // Newline after each row
    }
  }

  /**
   * Returns a raster that is converted from the geometry provided.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster
   * @param allTouched When set to true, rasterizes all pixels touched by geom
   * @param value The value of the pixel of the resultant raster
   * @param noDataValue The noDataValue of the resultant raster
   * @param useGeometryExtent The way to generate extent of the resultant raster. Use the extent of
   *     the geometry to convert if true, else use the extent of the reference raster
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      boolean allTouched,
      double value,
      Double noDataValue,
      boolean useGeometryExtent)
      throws FactoryException {

    //    System.out.println("\nnoDataValue: " + noDataValue);
    //    System.out.println("value: " + value);
    //    System.out.println("useGeometryExtent: " + useGeometryExtent);

    List<Object> objects1 =
        Rasterization.rasterize(geom, raster, pixelType, value, useGeometryExtent, allTouched);
    //    List<Object> objects2 =
    //        rasterization(geom, raster, pixelType, value, noDataValue, useGeometryExtent,
    // allTouched);
    //    List<Object> objects2 =
    //        Rasterization2.rasterize(geom, raster, pixelType, value, useGeometryExtent,
    // allTouched);

    WritableRaster writableRaster1 = (WritableRaster) objects1.get(0);
    GridCoverage2D rasterized1 = (GridCoverage2D) objects1.get(1);

    //    System.out.println("\nPrint writableRaster in asRaster: \n");
    //    printWritableRasterWithCoordinates(writableRaster1);
    //
    //    System.out.println("\nPrint bandAsArray in asRaster: \n");
    //    System.out.println(Arrays.toString(MapAlgebra.bandAsArray(rasterized1, 1)));

    //    WritableRaster writableRaster2 = (WritableRaster) objects2.get(0);
    //    GridCoverage2D rasterized2 = (GridCoverage2D) objects2.get(1);

    //    System.out.println(
    //        "\nRasterized1 metadata: " + Arrays.toString(RasterAccessors.metadata(rasterized1)));
    //    System.out.println(
    //        "\nRasterized1 Envelope: " +
    // Functions.asEWKT(GeometryFunctions.envelope(rasterized1)));
    //    System.out.println(
    //        "Rasterized1 band 1: " + Arrays.toString(MapAlgebra.bandAsArray(rasterized1, 1)));

    //    System.out.println(
    //        "\nRasterized2 metadata: " + Arrays.toString(RasterAccessors.metadata(rasterized2)));
    //    System.out.println(
    //        "Rasterized2 band 1: " + Arrays.toString(MapAlgebra.bandAsArray(rasterized2, 1)));

    //    // Path to the output CSV file
    //    String filePath = "/Users/pranavtoggi/Downloads/rasterized_output.csv";
    //
    //    // Save the double array as a CSV file
    //    saveDoubleArrayAsCSV(MapAlgebra.bandAsArray(rasterized1, 1), filePath);

    GridCoverage2D resultRaster1 =
        RasterUtils.clone(
            writableRaster1,
            rasterized1.getSampleDimensions(),
            rasterized1,
            noDataValue,
            false); // no need to original raster metadata since this is a new raster.
    //    GridCoverage2D resultRaster2 =
    //        RasterUtils.clone(
    //            writableRaster2,
    //            rasterized2.getSampleDimensions(),
    //            rasterized2,
    //            noDataValue,
    //            false); // no need to original raster metadata since this is a new raster.

    if (noDataValue != null) {
      //      System.out.println("\nnoDataValue != null");
      resultRaster1 = RasterBandEditors.setBandNoDataValue(resultRaster1, 1, noDataValue);
    }

    //    System.out.println("\nPrint bandAsArray after cloning in asRaster: \n");
    //    System.out.println(Arrays.toString(MapAlgebra.bandAsArray(resultRaster1, 1)));
    //    if (noDataValue != null) {
    //      resultRaster2 = RasterBandEditors.setBandNoDataValue(resultRaster2, 1, noDataValue);
    //    }

    //    System.out.println(
    //        "\nresultRaster1 Envelope: " +
    // Functions.asEWKT(GeometryFunctions.envelope(resultRaster1)));
    //    System.out.println(
    //        "resultRaster2 Envelope: " +
    // Functions.asEWKT(GeometryFunctions.envelope(resultRaster2)));
    //
    //    System.out.println(
    //        "\nresultRaster1 metadata: " +
    // Arrays.toString(RasterAccessors.metadata(resultRaster1)));
    //    System.out.println(
    //        "resultRaster2 metadata: " +
    // Arrays.toString(RasterAccessors.metadata(resultRaster2)));
    //
    //    System.out.println(
    //        "\nRasterized1 Metadata: " +
    // Arrays.toString(RasterAccessors.metadata(resultRaster1)));
    //    System.out.println(
    //        "Rasterized2 Metadata: " + Arrays.toString(RasterAccessors.metadata(resultRaster2)));

    //        double[] o1 = MapAlgebra.bandAsArray(resultRaster1, 1);
    //        double[] o2 = MapAlgebra.bandAsArray(resultRaster2, 1);

    //    System.out.println("\nLength o1: " + o1.length);
    //    System.out.println("Length o2: " + o2.length);

    //    assert Arrays.equals(o1, o2) : "\nArrays are not equal";
    //    System.out.println("\nArrays are equal!");

    //    List<String> differences = findDifferences(o1, o2);

    //    if (differences.isEmpty()) {
    //      System.out.println("\nArrays are equal!");
    //    } else {
    //      System.out.println("\nDifferences found:");
    //      differences.forEach(System.out::println);
    //    }

    //    System.out.println("\nLength of differences: " + differences.size());

    //    System.out.println(
    //        "\nRaster metadata: " + Arrays.toString(RasterAccessors.metadata(resultRaster2)));
    //    System.out.println(
    //        "\nRasterized Raster: " + Arrays.toString(MapAlgebra.bandAsArray(resultRaster1, 1)));
    // Path to the output CSV file
    //    String filePath = "/Users/pranavtoggi/Downloads/rasterized_output.csv";

    // Save the double array as a CSV file
    //    saveDoubleArrayAsCSV(MapAlgebra.bandAsArray(resultRaster2, 1), filePath);

    return resultRaster1;
  }

  public static void saveDoubleArrayAsCSV(double[] array, String filePath) {
    try (FileWriter writer = new FileWriter(filePath)) {
      // Convert double array to a CSV format
      String csvString =
          Arrays.toString(array)
              .replace("[", "") // Remove opening bracket
              .replace("]", ""); // Remove closing bracket

      // Write to file
      writer.write(csvString);
      writer.flush();

      System.out.println("Array successfully saved as a CSV in: " + filePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static List<String> findDifferences(double[] arr1, double[] arr2) {
    List<String> differences = new ArrayList<>();

    int minLength = Math.min(arr1.length, arr2.length);
    for (int i = 0; i < minLength; i++) {
      if (Math.abs(arr1[i] - arr2[i]) > 1e-15) { // Check precision difference
        differences.add("Index " + i + ": " + arr1[i] + " != " + arr2[i]);
      }
    }

    // Handle extra elements in the longer array
    if (arr1.length > arr2.length) {
      for (int i = minLength; i < arr1.length; i++) {
        differences.add("Index " + i + ": Extra element in arr1 -> " + arr1[i]);
      }
    } else if (arr2.length > arr1.length) {
      for (int i = minLength; i < arr2.length; i++) {
        differences.add("Index " + i + ": Extra element in arr2 -> " + arr2[i]);
      }
    }

    return differences;
  }

  //  public static GridCoverage2D asRaster(
  //      Geometry geom,
  //      GridCoverage2D raster,
  //      String pixelType,
  //      boolean allTouched,
  //      double value,
  //      Double noDataValue,
  //      boolean useGeometryExtent)
  //      throws FactoryException {
  //    List<Object> objects =
  //        rasterization(geom, raster, pixelType, value, noDataValue, useGeometryExtent,
  // allTouched);
  //    WritableRaster writableRaster = (WritableRaster) objects.get(0);
  //    GridCoverage2D rasterized = (GridCoverage2D) objects.get(1);
  //
  //    return RasterUtils.clone(
  //        writableRaster,
  //        rasterized.getSampleDimensions(),
  //        rasterized,
  //        noDataValue,
  //        false); // no need to original raster metadata since this is a new raster.
  //  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster
   * @param allTouched When set to true, rasterizes all pixels touched by geom
   * @param value The value of the pixel of the resultant raster
   * @param noDataValue The noDataValue of the resultant raster
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      boolean allTouched,
      double value,
      Double noDataValue)
      throws FactoryException {
    GridCoverage2D result = asRaster(geom, raster, pixelType, allTouched, value, noDataValue, true);
    return result;
  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster.
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(Geometry geom, GridCoverage2D raster, String pixelType)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, false, 1, null);
  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster.
   * @param allTouched When set to true, rasterizes all pixels touched by geom
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom, GridCoverage2D raster, String pixelType, boolean allTouched)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, allTouched, 1, null);
  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster.
   * @param allTouched When set to true, rasterizes all pixels touched by geom
   * @param value The value of the pixel of the resultant raster
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom, GridCoverage2D raster, String pixelType, boolean allTouched, double value)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, allTouched, value, null);
  }

  private static List<Object> rasterization(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      Double noDataValue,
      boolean useGeometryExtent,
      boolean allTouched)
      throws FactoryException {
    System.out.println("Rasterization 1...");
    DefaultFeatureCollection featureCollection =
        getFeatureCollection(geom, raster.getCoordinateReferenceSystem());

    double[] metadata = RasterAccessors.metadata(raster);
    // The current implementation doesn't support rasters with properties below
    // It is not a problem as most rasters don't have these properties
    // ScaleX < 0
    if (metadata[4] < 0) {
      throw new IllegalArgumentException(
          String.format("ScaleX %f of the raster is negative, it should be positive", metadata[4]));
    }
    // ScaleY > 0
    if (metadata[5] > 0) {
      throw new IllegalArgumentException(
          String.format(
              "ScaleY %f of the raster is positive. It should be negative.", metadata[5]));
    }
    // SkewX should be zero
    if (metadata[6] != 0) {
      throw new IllegalArgumentException(
          String.format("SkewX %d of the raster is not zero.", metadata[6]));
    }
    // SkewY should be zero
    if (metadata[7] != 0) {
      throw new IllegalArgumentException(
          String.format("SkewY %d of the raster is not zero.", metadata[7]));
    }

    Envelope2D bound = null;

    int width, height;
    if (useGeometryExtent) {
      bound =
          JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());
      double scaleX = Math.abs(metadata[4]), scaleY = Math.abs(metadata[5]);
      width = Math.max((int) Math.ceil(bound.getWidth() / scaleX), 1);
      height = Math.max((int) Math.ceil(bound.getHeight() / scaleY), 1);
      bound =
          new Envelope2D(
              bound.getCoordinateReferenceSystem(),
              bound.getMinX(),
              bound.getMinY(),
              width * scaleX,
              height * scaleY);
    } else {
      ReferencedEnvelope envelope =
          ReferencedEnvelope.create(raster.getEnvelope(), raster.getCoordinateReferenceSystem());
      bound = JTS.getEnvelope2D(envelope, raster.getCoordinateReferenceSystem2D());
      GridEnvelope2D gridRange = raster.getGridGeometry().getGridRange2D();
      width = gridRange.width;
      height = gridRange.height;
    }

    VectorToRasterProcess rasterProcess = new VectorToRasterProcess();
    GridCoverage2D rasterized =
        rasterProcess.execute(
            featureCollection, width, height, "value", Double.toString(value), bound, null);

    //    System.out.println(
    //        "\nBefore setting noDataValue: \n"
    //            + Arrays.toString(MapAlgebra.bandAsArray(rasterized, 1)));

    if (noDataValue != null) {
      rasterized = RasterBandEditors.setBandNoDataValue(rasterized, 1, noDataValue);
    }
    WritableRaster writableRaster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(pixelType), width, height, 1, null);
    double[] samples =
        RasterUtils.getRaster(rasterized.getRenderedImage())
            .getSamples(0, 0, width, height, 0, (double[]) null);
    writableRaster.setSamples(0, 0, width, height, 0, samples);

    List<Object> objects = new ArrayList<>();
    objects.add(writableRaster);
    objects.add(rasterized);

    return objects;
  }

  private static List<Object> rasterization2(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      Double noDataValue,
      boolean useGeometryExtent,
      boolean allTouched)
      throws FactoryException {
    System.out.println("Rasterization 2...");
    DefaultFeatureCollection featureCollection =
        getFeatureCollection(geom, raster.getCoordinateReferenceSystem());

    double[] metadata = RasterAccessors.metadata(raster);

    // Validate raster properties
    if (metadata[4] < 0) {
      throw new IllegalArgumentException(
          String.format("ScaleX %f of the raster is negative, it should be positive", metadata[4]));
    }
    // ScaleY > 0
    if (metadata[5] > 0) {
      throw new IllegalArgumentException(
          String.format(
              "ScaleY %f of the raster is positive. It should be negative.", metadata[5]));
    }
    // SkewX should be zero
    if (metadata[6] != 0) {
      throw new IllegalArgumentException(
          String.format("SkewX %d of the raster is not zero.", metadata[6]));
    }
    // SkewY should be zero
    if (metadata[7] != 0) {
      throw new IllegalArgumentException(
          String.format("SkewY %d of the raster is not zero.", metadata[7]));
    }

    Envelope2D bound = null;

    int width, height;

    // Determine raster bounds and dimensions
    if (useGeometryExtent) {
      bound =
          JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());
      double scaleX = Math.abs(metadata[4]), scaleY = Math.abs(metadata[5]);
      width = Math.max((int) Math.ceil(bound.getWidth() / scaleX), 1);
      height = Math.max((int) Math.ceil(bound.getHeight() / scaleY), 1);
      bound =
          new Envelope2D(
              bound.getCoordinateReferenceSystem(),
              bound.getMinX(),
              bound.getMinY(),
              width * scaleX,
              height * scaleY);
    } else {
      ReferencedEnvelope envelope =
          ReferencedEnvelope.create(raster.getEnvelope(), raster.getCoordinateReferenceSystem());
      bound = JTS.getEnvelope2D(envelope, raster.getCoordinateReferenceSystem2D());
      GridEnvelope2D gridRange = raster.getGridGeometry().getGridRange2D();
      width = gridRange.width;
      height = gridRange.height;
    }

    WritableRaster writableRaster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(pixelType), width, height, 1, null);

    // Iterate over grid cells and rasterize
    double cellWidth = bound.getWidth() / width;
    double cellHeight = bound.getHeight() / height;

    for (int x = 0; x < width; x++) {
      for (int y = 0; y < height; y++) {
        double minX = bound.getMinX() + x * cellWidth;
        double maxX = minX + cellWidth;
        double maxY = bound.getMaxY() - y * cellHeight;
        double minY = maxY - cellHeight;

        Envelope2D cellEnvelope =
            new Envelope2D(bound.getCoordinateReferenceSystem(), minX, minY, cellWidth, cellHeight);
        Geometry cellGeometry = JTS.toGeometry((Shape) cellEnvelope);

        // Determine if the cell is touched based on allTouched parameter
        boolean isTouched =
            allTouched ? geom.intersects(cellGeometry) : geom.contains(cellGeometry.getCentroid());

        writableRaster.setSample(
            x, y, 0, isTouched ? value : (noDataValue != null ? noDataValue : 0));
      }
    }

    // Create GridCoverage2D using RasterUtils
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, width, height),
            new AffineTransform2D(
                metadata[4],
                metadata[7],
                metadata[6],
                metadata[5],
                bound.getMinX(),
                bound.getMaxY()),
            raster.getCoordinateReferenceSystem());
    GridCoverage2D rasterized =
        RasterUtils.create(writableRaster, gridGeometry, raster.getSampleDimensions(), noDataValue);

    // Return results as List<Object>
    List<Object> objects = new ArrayList<>();
    objects.add(writableRaster);
    objects.add(rasterized);

    return objects;
  }

  private static List<Object> rasterization3(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      Double noDataValue,
      boolean useGeometryExtent,
      boolean allTouched)
      throws FactoryException {

    //    System.out.println("Custom Rasterization...");
    //    System.out.println("allTouched = " + allTouched);

    // Step 1: Validate the input geometry and raster metadata
    double[] metadata = RasterAccessors.metadata(raster);
    validateRasterMetadata(metadata);

    // Step 2: Define and align raster grid properties
    Envelope2D rasterExtent =
        useGeometryExtent
            ? JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D())
            : JTS.getEnvelope2D(
                ReferencedEnvelope.create(
                    raster.getEnvelope(), raster.getCoordinateReferenceSystem()),
                raster.getCoordinateReferenceSystem2D());

    double scaleX = Math.abs(metadata[4]);
    double scaleY = Math.abs(metadata[5]);

    // Align the raster grid to the resolution
    double alignedMinX = Math.floor(rasterExtent.getMinX() / scaleX) * scaleX;
    double alignedMinY = Math.floor(rasterExtent.getMinY() / scaleY) * scaleY;
    double alignedMaxX = Math.ceil(rasterExtent.getMaxX() / scaleX) * scaleX;
    double alignedMaxY = Math.ceil(rasterExtent.getMaxY() / scaleY) * scaleY;

    // Adjust the raster extent
    Envelope2D alignedRasterExtent =
        new Envelope2D(
            rasterExtent.getCoordinateReferenceSystem(),
            alignedMinX,
            alignedMinY,
            alignedMaxX - alignedMinX,
            alignedMaxY - alignedMinY);

    // Define raster dimensions based on the aligned extent
    int rasterWidth = (int) Math.ceil((alignedMaxX - alignedMinX) / scaleX);
    int rasterHeight = (int) Math.ceil((alignedMaxY - alignedMinY) / scaleY);

    WritableRaster writableRaster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(pixelType), rasterWidth, rasterHeight, 1, null);

    //    System.out.println("  Raster extent min: " + alignedMinX + "," + alignedMinY + " - ");
    //    System.out.println("  Raster extent max: " + alignedMaxX + "," + alignedMaxY + " - ");
    //
    //    System.out.println("\nupperLeftX: " + metadata[0]);
    //    System.out.println("upperLeftY: " + metadata[1]);
    //    System.out.println("rasterHeight: " + rasterHeight);
    //    System.out.println("rasterWidth: " + rasterWidth);

    // Step 3: Iterate over raster grid and rasterize the geometry
    for (int y = 0; y < rasterHeight; y++) {
      for (int x = 0; x < rasterWidth; x++) {
        // Compute cell bounds
        double cellMinX = alignedMinX + x * scaleX;
        double cellMaxX = cellMinX + scaleX;
        double cellMaxY = alignedMinY + y * scaleY;
        double cellMinY = cellMaxY + scaleY;

        //        int yIndex = rasterHeight - y - 1; // Flip the y-axis
        //
        //        double cellMinX = alignedMinX + x * scaleX;
        //        double cellMaxX = cellMinX + scaleX;
        //        double cellMaxY = alignedMinY + yIndex * scaleY;
        //        double cellMinY = cellMaxY + scaleY;

        //        System.out.println(
        //            "\ncellMinX = "
        //                + cellMinX
        //                + "; cellMaxX = "
        //                + cellMaxX
        //                + "; cellMinY = "
        //                + cellMinY
        //                + "; cellMaxY = "
        //                + cellMaxY);

        Envelope cellEnvelope = new Envelope(cellMinX, cellMaxX, cellMinY, cellMaxY);
        Geometry cellGeometry = JTS.toGeometry(cellEnvelope);

        // Check intersection between geometry and raster cell
        boolean intersects =
            allTouched
                ? geom.intersects(cellGeometry) // Mark cell if any part intersects
                : geom.intersects(
                    cellGeometry.getCentroid()); // Mark cell only if centroid is inside

        //        System.out.println(
        //            "Cell centroid for "
        //                + cellMinX
        //                + ","
        //                + cellMinY
        //                + ": "
        //                + Functions.asEWKT(cellGeometry.getCentroid())
        //                + "; x,y = "
        //                + x
        //                + ","
        //                + y
        //                + "; intersects geom"
        //                + " = "
        //                + geom.intersects(cellGeometry)
        //                + "; intersects centroid = "
        //                + geom.intersects(cellGeometry.getCentroid())
        //                + "; intersects"
        //                + " = "
        //                + intersects
        //                + "; "
        //                + (int) (cellMinX / scaleX)
        //                + ", "
        //                + (int) (-cellMinY / scaleY));

        if (intersects) {
          //          writableRaster.setSample(
          //              (int) (cellMinX / scaleX),
          //              (int) (-cellMinY / scaleY),
          //              0,
          //              value); // Set the cell value
          writableRaster.setSample(x, y, 0, value);
        } else if (noDataValue != null) {
          writableRaster.setSample(x, y, 0, noDataValue); // Set NoData value
        }
      }
    }

    //    for (int y = 0; y < rasterHeight; y++) {
    //      // Correct the y-index computation to invert the y-axis
    //      int yIndex = rasterHeight - y - 1; // Flip the y-axis for raster indexing
    //
    //      for (int x = 0; x < rasterWidth; x++) {
    //        double cellMinX = alignedMinX + x * scaleX;
    //        double cellMaxX = cellMinX + scaleX;
    //
    //        // Use yIndex to calculate the cell boundaries
    //        double cellMaxY = alignedMinY + yIndex * scaleY;
    //        double cellMinY = cellMaxY - scaleY; // Subtract to maintain correct extent
    //
    //        // Debugging output to verify cell bounds
    //        System.out.println(
    //            "\ncellMinX = "
    //                + cellMinX
    //                + "; cellMaxX = "
    //                + cellMaxX
    //                + "; cellMinY = "
    //                + cellMinY
    //                + "; cellMaxY = "
    //                + cellMaxY);
    //
    //        Envelope cellEnvelope = new Envelope(cellMinX, cellMaxX, cellMinY, cellMaxY);
    //        Geometry cellGeometry = JTS.toGeometry(cellEnvelope);
    //
    //        // Check intersection between geometry and raster cell
    //        boolean intersects =
    //            allTouched
    //                ? geom.intersects(cellGeometry) // Mark cell if any part intersects
    //                : geom.intersects(
    //                    cellGeometry.getCentroid()); // Mark cell only if centroid is inside
    //
    //        System.out.println(
    //            "Cell centroid for "
    //                + cellMinX
    //                + ","
    //                + cellMinY
    //                + ": "
    //                + Functions.asEWKT(cellGeometry.getCentroid())
    //                + "; x,y = "
    //                + x
    //                + ","
    //                + yIndex
    //                + "; intersects geom = "
    //                + geom.intersects(cellGeometry)
    //                + "; intersects centroid = "
    //                + geom.intersects(cellGeometry.getCentroid())
    //                + "; intersects = "
    //                + intersects);
    //
    //        if (intersects) {
    //          writableRaster.setSample(x, yIndex, 0, value); // Use yIndex for raster indexing
    //        } else if (noDataValue != null) {
    //          writableRaster.setSample(x, yIndex, 0, noDataValue); // Use yIndex for raster
    // indexing
    //        }
    //      }
    //    }

    // Step 4: Create a GridCoverage2D for the rasterized result
    GridCoverageFactory coverageFactory = new GridCoverageFactory();
    GridCoverage2D rasterized =
        coverageFactory.create("rasterized", writableRaster, alignedRasterExtent);

    // Step 5: Return results compatible with the original function
    List<Object> objects = new ArrayList<>();
    objects.add(writableRaster);
    objects.add(rasterized);

    return objects;
  }

  private static void validateRasterMetadata(double[] metadata) {
    if (metadata[4] < 0) {
      throw new IllegalArgumentException("ScaleX cannot be negative");
    }
    if (metadata[5] > 0) {
      throw new IllegalArgumentException("ScaleY must be negative");
    }
    if (metadata[6] != 0 || metadata[7] != 0) {
      throw new IllegalArgumentException("SkewX and SkewY must be zero");
    }
  }

  /**
   * For internal use only! Returns a raster that is converted from the geometry provided with the
   * extent of the reference raster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster
   * @param value The value of the pixel of the resultant raster
   * @param noDataValue The noDataValue of the resultant raster
   * @return Rasterized Geometry with reference raster's extent
   * @throws FactoryException
   */
  public static GridCoverage2D asRasterWithRasterExtent(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      boolean allTouched,
      double value,
      Double noDataValue)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, allTouched, value, noDataValue, false);
  }

  public static DefaultFeatureCollection getFeatureCollection(
      Geometry geom, CoordinateReferenceSystem crs) {
    SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName("Raster");
    simpleFeatureTypeBuilder.setCRS(crs);
    simpleFeatureTypeBuilder.add("geometry", Geometry.class);

    SimpleFeatureType featureType = simpleFeatureTypeBuilder.buildFeatureType();
    SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
    featureBuilder.add(geom);
    SimpleFeature simpleFeature = featureBuilder.buildFeature("1");
    DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
    featureCollection.add(simpleFeature);

    return featureCollection;
  }

  /**
   * Convenience function setting DOUBLE as datatype for the bands Create a new empty raster with
   * the given number of empty bands. The bounding envelope is defined by the upper left corner and
   * the scale. The math formula of the envelope is: minX = upperLeftX = lowerLeftX, minY
   * (lowerLeftY) = upperLeftY - height * pixelSize
   *
   * <ul>
   *   <li>The raster is defined by the width and height
   *   <li>The upper left corner is defined by the upperLeftX and upperLeftY
   *   <li>The scale is defined by pixelSize. The scaleX is equal to pixelSize and scaleY is equal
   *       to -pixelSize
   *   <li>skewX and skewY are zero, which means no shear or rotation.
   *   <li>SRID is default to 0 which means the default CRS (Generic 2D)
   * </ul>
   *
   * @param numBand the number of bands
   * @param widthInPixel the width of the raster, in pixel
   * @param heightInPixel the height of the raster, in pixel
   * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is
   *     equal to the upperLeftX
   * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is
   *     equal to the upperLeftY - height * pixelSize
   * @param pixelSize the size of the pixel in the unit of the CRS
   * @return the new empty raster
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double pixelSize)
      throws FactoryException {
    return makeEmptyRaster(
        numBand,
        widthInPixel,
        heightInPixel,
        upperLeftX,
        upperLeftY,
        pixelSize,
        -pixelSize,
        0,
        0,
        0);
  }

  /**
   * Convenience function allowing explicitly setting the datatype for all the bands
   *
   * @param numBand
   * @param dataType
   * @param widthInPixel
   * @param heightInPixel
   * @param upperLeftX
   * @param upperLeftY
   * @param pixelSize
   * @return
   * @throws FactoryException
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      String dataType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double pixelSize)
      throws FactoryException {
    return makeEmptyRaster(
        numBand,
        dataType,
        widthInPixel,
        heightInPixel,
        upperLeftX,
        upperLeftY,
        pixelSize,
        -pixelSize,
        0,
        0,
        0);
  }

  /**
   * Convenience function for creating a raster with data type DOUBLE for all the bands
   *
   * @param numBand
   * @param widthInPixel
   * @param heightInPixel
   * @param upperLeftX
   * @param upperLeftY
   * @param scaleX
   * @param scaleY
   * @param skewX
   * @param skewY
   * @param srid
   * @return
   * @throws FactoryException
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid)
      throws FactoryException {
    return makeEmptyRaster(
        numBand,
        "d",
        widthInPixel,
        heightInPixel,
        upperLeftX,
        upperLeftY,
        scaleX,
        scaleY,
        skewX,
        skewY,
        srid);
  }

  /**
   * Create a new empty raster with the given number of empty bands
   *
   * @param numBand the number of bands
   * @param bandDataType the data type of the raster, one of D | B | I | F | S | US
   * @param widthInPixel the width of the raster, in pixel
   * @param heightInPixel the height of the raster, in pixel
   * @param upperLeftX the upper left corner of the raster, in the CRS unit. Note that: the minX of
   *     the envelope is equal to the upperLeftX
   * @param upperLeftY the upper left corner of the raster, in the CRS unit. Note that: the minY of
   *     the envelope is equal to the upperLeftY + height * scaleY
   * @param scaleX the scale of the raster (pixel size on X), in the CRS unit
   * @param scaleY the scale of the raster (pixel size on Y), in the CRS unit
   * @param skewX the skew of the raster on X, in the CRS unit
   * @param skewY the skew of the raster on Y, in the CRS unit
   * @param srid the srid of the CRS. 0 means the default CRS (Cartesian 2D)
   * @return the new empty raster
   * @throws FactoryException
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      String bandDataType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid)
      throws FactoryException {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = FunctionsGeoTools.sridToCRS(srid);
    }

    // Create a new empty raster
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBand, null);
    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);
    return RasterUtils.create(raster, gridGeometry, null, null);
  }

  public static GridCoverage2D makeNonEmptyRaster(
      int numBand,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid,
      double[][] data,
      Map<String, List<String>> properties,
      Double noDataValue,
      PixelInCell anchor)
      throws FactoryException {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = CRS.decode("EPSG:" + srid, true);
    }

    // Create a new empty raster
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            5, widthInPixel, heightInPixel, numBand, null); // create a raster with double values
    for (int i = 0; i < numBand; i++) {
      raster.setSamples(0, 0, widthInPixel, heightInPixel, i, data[i]);
    }
    // raster.setPixels(0, 0, widthInPixel, heightInPixel, data);
    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel), anchor, transform, crs, null);
    return RasterUtils.create(raster, gridGeometry, null, noDataValue, properties);
  }

  public static GridCoverage2D makeNonEmptyRaster(
      int numBands,
      String bandDataType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid,
      double[][] rasterValues) {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = FunctionsGeoTools.sridToCRS(srid);
    }

    // Create a new empty raster
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBands, null);
    for (int i = 0; i < numBands; i++)
      raster.setSamples(0, 0, widthInPixel, heightInPixel, i, rasterValues[i]);
    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);
    return RasterUtils.create(raster, gridGeometry, null);
  }

  /**
   * Make a non-empty raster from a reference raster and a set of values. The constructed raster
   * will have the same CRS, geo-reference metadata, width and height as the reference raster. The
   * number of bands of the reference raster is determined by the size of values. The size of values
   * should be multiple of width * height of the reference raster.
   *
   * @param ref the reference raster
   * @param bandDataType the data type of the band
   * @param values the values to set
   * @return the constructed raster
   */
  public static GridCoverage2D makeNonEmptyRaster(
      GridCoverage2D ref, String bandDataType, double[] values) {
    CoordinateReferenceSystem crs = ref.getCoordinateReferenceSystem();
    int widthInPixel = ref.getRenderedImage().getWidth();
    int heightInPixel = ref.getRenderedImage().getHeight();
    int valuesPerBand = widthInPixel * heightInPixel;
    if (values.length == 0) {
      throw new IllegalArgumentException("The size of values should be greater than 0");
    }
    if (values.length % valuesPerBand != 0) {
      throw new IllegalArgumentException(
          "The size of values should be multiple of width * height of the reference raster");
    }
    int numBands = values.length / valuesPerBand;
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBands, null);
    for (int i = 0; i < numBands; i++) {
      double[] bandValues = Arrays.copyOfRange(values, i * valuesPerBand, (i + 1) * valuesPerBand);
      raster.setSamples(0, 0, widthInPixel, heightInPixel, i, bandValues);
    }
    MathTransform transform = ref.getGridGeometry().getGridToCRS(PixelInCell.CELL_CENTER);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CENTER,
            transform,
            crs,
            null);
    return RasterUtils.create(raster, gridGeometry, null);
  }

  public static class Tile {
    private final int tileX;
    private final int tileY;
    private final GridCoverage2D coverage;

    public Tile(int tileX, int tileY, GridCoverage2D coverage) {
      this.tileX = tileX;
      this.tileY = tileY;
      this.coverage = coverage;
    }

    public int getTileX() {
      return tileX;
    }

    public int getTileY() {
      return tileY;
    }

    public GridCoverage2D getCoverage() {
      return coverage;
    }
  }

  /**
   * Generate tiles from a grid coverage
   *
   * @param gridCoverage2D the grid coverage
   * @param bandIndices the indices of the bands to select (1-based), can be null or empty to
   *     include all the bands.
   * @param tileWidth the width of the tiles
   * @param tileHeight the height of the tiles
   * @param padWithNoData whether to pad the tiles with no data value
   * @param padNoDataValue the no data value for padded tiles, only used when padWithNoData is true.
   *     If the value is NaN, the no data value of the original band will be used.
   * @return the tiles
   */
  public static Tile[] generateTiles(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData,
      double padNoDataValue) {
    int numBands = gridCoverage2D.getNumSampleDimensions();
    if (bandIndices == null || bandIndices.length == 0) {
      // Select all the bands
      bandIndices = new int[numBands];
      for (int i = 0; i < numBands; i++) {
        bandIndices[i] = i + 1;
      }
    } else {
      // Check the band indices
      for (int bandIndex : bandIndices) {
        if (bandIndex <= 0 || bandIndex > numBands) {
          throw new IllegalArgumentException(
              String.format("Provided band index %d is not present in the raster", bandIndex));
        }
      }
    }
    return doGenerateTiles(
        gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, padNoDataValue);
  }

  /**
   * Generate tiles from an in-db grid coverage. The generated tiles are also in-db grid coverages.
   * Pixel data will be copied into the tiles.
   *
   * @param gridCoverage2D the in-db grid coverage
   * @param bandIndices the indices of the bands to select (1-based)
   * @param tileWidth the width of the tiles
   * @param tileHeight the height of the tiles
   * @param padWithNoData whether to pad the tiles with no data value
   * @param padNoDataValue the no data value for padded tiles, only used when padWithNoData is true.
   *     If the value is NaN, the no data value of the original band will be used.
   * @return the tiles
   */
  private static Tile[] doGenerateTiles(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData,
      double padNoDataValue) {
    AffineTransform2D affine =
        RasterUtils.getAffineTransform(gridCoverage2D, PixelOrientation.CENTER);
    RenderedImage image = gridCoverage2D.getRenderedImage();
    double[] noDataValues = new double[bandIndices.length];
    for (int i = 0; i < bandIndices.length; i++) {
      noDataValues[i] =
          RasterUtils.getNoDataValue(gridCoverage2D.getSampleDimension(bandIndices[i] - 1));
    }
    int width = image.getWidth();
    int height = image.getHeight();
    int numTileX = (int) Math.ceil((double) width / tileWidth);
    int numTileY = (int) Math.ceil((double) height / tileHeight);
    Tile[] tiles = new Tile[numTileX * numTileY];
    for (int tileY = 0; tileY < numTileY; tileY++) {
      for (int tileX = 0; tileX < numTileX; tileX++) {
        int x0 = tileX * tileWidth;
        int y0 = tileY * tileHeight;

        // Rect to copy from the original image
        int rectWidth = Math.min(tileWidth, width - x0);
        int rectHeight = Math.min(tileHeight, height - y0);

        // If we don't pad with no data, the tiles on the boundary may have a different size
        int currentTileWidth = padWithNoData ? tileWidth : rectWidth;
        int currentTileHeight = padWithNoData ? tileHeight : rectHeight;
        boolean needPadding = padWithNoData && (rectWidth < tileWidth || rectHeight < tileHeight);

        // Create a new affine transformation for this tile
        AffineTransform2D tileAffine = RasterUtils.translateAffineTransform(affine, x0, y0);
        GridGeometry2D gridGeometry2D =
            new GridGeometry2D(
                new GridEnvelope2D(0, 0, currentTileWidth, currentTileHeight),
                PixelInCell.CELL_CENTER,
                tileAffine,
                gridCoverage2D.getCoordinateReferenceSystem(),
                null);

        // Prepare a new image for this tile, and copy the data from the original image
        WritableRaster raster =
            RasterFactory.createBandedRaster(
                image.getSampleModel().getDataType(),
                currentTileWidth,
                currentTileHeight,
                bandIndices.length,
                null);
        GridSampleDimension[] sampleDimensions = new GridSampleDimension[bandIndices.length];
        Raster sourceRaster = image.getData(new Rectangle(x0, y0, rectWidth, rectHeight));
        for (int k = 0; k < bandIndices.length; k++) {
          int bandIndex = bandIndices[k] - 1;

          // Copy sample dimensions from source bands, and pad with no data value if necessary
          GridSampleDimension sampleDimension = gridCoverage2D.getSampleDimension(bandIndex);
          double noDataValue = noDataValues[k];
          if (needPadding && !Double.isNaN(padNoDataValue)) {
            sampleDimension =
                RasterUtils.createSampleDimensionWithNoDataValue(sampleDimension, padNoDataValue);
            noDataValue = padNoDataValue;
          }
          sampleDimensions[k] = sampleDimension;

          // Copy data from original image to tile image
          ImageUtils.copyRasterWithPadding(sourceRaster, bandIndex, raster, k, noDataValue);
        }

        GridCoverage2D tile = RasterUtils.create(raster, gridGeometry2D, sampleDimensions);
        tiles[tileY * numTileX + tileX] = new Tile(tileX, tileY, tile);
      }
    }

    return tiles;
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData,
      Double padNoDataValue) {
    if (gridCoverage2D == null) {
      return null;
    }
    if (padNoDataValue == null) {
      padNoDataValue = Double.NaN;
    }
    Tile[] tiles =
        generateTiles(
            gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, padNoDataValue);
    GridCoverage2D[] result = new GridCoverage2D[tiles.length];
    for (int i = 0; i < tiles.length; i++) {
      result[i] = tiles[i].getCoverage();
    }
    return result;
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData) {
    return rsTile(gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, Double.NaN);
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D, int[] bandIndices, int tileWidth, int tileHeight) {
    return rsTile(gridCoverage2D, bandIndices, tileWidth, tileHeight, false);
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D, int tileWidth, int tileHeight) {
    return rsTile(gridCoverage2D, null, tileWidth, tileHeight);
  }
}
