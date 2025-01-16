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

import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.List;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.opengis.referencing.FactoryException;

public class Rasterization {
  protected static List<Object> rasterize(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      boolean useGeometryExtent,
      boolean allTouched)
      throws FactoryException {

    System.out.println("Custom Rasterization with Line Support...");
    System.out.println("allTouched = " + allTouched);

    // Step 1: Validate the input geometry and raster metadata
    double[] metadata = RasterAccessors.metadata(raster);
    validateRasterMetadata(metadata);

    // Step 2: Define and align raster grid properties
    RasterizationParams params =
        calculateRasterizationParams(geom, raster, metadata, useGeometryExtent, pixelType);

    // Step 3: Rasterize the geometry based on its type
    if (geom.getGeometryType().equalsIgnoreCase("LineString")
        || geom.getGeometryType().equalsIgnoreCase("MultiLineString")) {
      rasterizeLineString(
          geom,
          params.writableRaster,
          params.alignedRasterExtent,
          params.scaleX,
          params.scaleY,
          value);
    } else {
      rasterizePolygon(
          geom,
          params.writableRaster,
          params.alignedRasterExtent,
          params.scaleX,
          params.scaleY,
          value,
          allTouched);
    }

    // Step 4: Create a GridCoverage2D for the rasterized result
    GridCoverageFactory coverageFactory = new GridCoverageFactory();
    GridCoverage2D rasterized =
        coverageFactory.create("rasterized", params.writableRaster, params.alignedRasterExtent);

    // Step 5: Return results compatible with the original function
    List<Object> objects = new ArrayList<>();
    objects.add(params.writableRaster);
    objects.add(rasterized);

    return objects;
  }

  private static void rasterizePolygon(
      Geometry geom,
      WritableRaster writableRaster,
      Envelope2D rasterExtent,
      double scaleX,
      double scaleY,
      double value,
      boolean allTouched) {

    int rasterWidth = writableRaster.getWidth();
    int rasterHeight = writableRaster.getHeight();

    for (int y = 0; y < rasterHeight; y++) {
      int yIndex = rasterHeight - y - 1; // Flip the y-axis

      for (int x = 0; x < rasterWidth; x++) {
        double cellMinX = rasterExtent.getMinX() + x * scaleX;
        double cellMaxX = cellMinX + scaleX;
        double cellMaxY = rasterExtent.getMinY() + y * scaleY;
        double cellMinY = cellMaxY + scaleY;

        System.out.println(
            "\ncellMinX = "
                + cellMinX
                + "; cellMaxX = "
                + cellMaxX
                + "; cellMinY = "
                + cellMinY
                + "; cellMaxY = "
                + cellMaxY);

        Envelope cellEnvelope = new Envelope(cellMinX, cellMaxX, cellMinY, cellMaxY);
        Geometry cellGeometry = JTS.toGeometry(cellEnvelope);

        boolean intersects =
            allTouched
                ? geom.intersects(cellGeometry)
                : geom.intersects(cellGeometry.getCentroid());

        System.out.println(
            "Cell centroid for "
                + cellMinX
                + ","
                + cellMinY
                + ": "
                + Functions.asEWKT(cellGeometry.getCentroid())
                + "; x,y = "
                + x
                + ","
                + y
                + "; intersects geom"
                + " = "
                + geom.intersects(cellGeometry)
                + "; intersects centroid = "
                + geom.intersects(cellGeometry.getCentroid())
                + "; intersects"
                + " = "
                + intersects
                + "; "
                + x
                + ", "
                + yIndex);

        if (intersects) {
          writableRaster.setSample(x, yIndex, 0, value);
        }
      }
    }
    System.out.println("\nwritableRaster after polyfill:");
    printWritableRasterWithCoordinates(writableRaster);
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

  private static void rasterizeLineString(
      Geometry geom,
      WritableRaster writableRaster,
      Envelope2D rasterExtent,
      double scaleX,
      double scaleY,
      double value) {

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      LineString line = (LineString) geom.getGeometryN(i);
      Coordinate[] coords = line.getCoordinates();

      for (int j = 0; j < coords.length - 1; j++) {
        // Extract start and end points for the segment
        Coordinate start = coords[j];
        Coordinate end = coords[j + 1];

        double x0 = (start.x - rasterExtent.getMinX()) / scaleX;
        double y0 = (rasterExtent.getMaxY() - start.y) / scaleY;
        double x1 = (end.x - rasterExtent.getMinX()) / scaleX;
        double y1 = (rasterExtent.getMaxY() - end.y) / scaleY;

        // Debug information
        System.out.printf(
            "Rasterizing segment: Start (%6.2f, %6.2f), End (%6.2f, %6.2f)%n", x0, y0, x1, y1);

        // Apply Bresenham for this segment
        drawLineBresenham(writableRaster, x0, y0, x1, y1, value, 0.5);
      }
    }

    System.out.println("\nWritable Raster after rasterizing LineString:");
    printWritableRasterWithCoordinates(writableRaster);
  }

  // Modified Bresenham with Fractional Steps
  private static void drawLineBresenham(
      WritableRaster raster,
      double x0,
      double y0,
      double x1,
      double y1,
      double value,
      double stepSize) {

    double dx = x1 - x0;
    double dy = y1 - y0;

    // Compute the number of steps based on the larger of dx or dy
    double distance = Math.sqrt(dx * dx + dy * dy);
    int steps = (int) Math.ceil(distance / stepSize);

    // Calculate the step increment for each axis
    double stepX = dx / steps;
    double stepY = dy / steps;

    // Start stepping through the line
    double x = x0;
    double y = y0;

    for (int i = 0; i <= steps; i++) {
      int rasterX = (int) Math.floor(x);
      int rasterY = (int) Math.floor(y);

      // Only write if within raster bounds
      if (rasterX >= 0
          && rasterX < raster.getWidth()
          && rasterY >= 0
          && rasterY < raster.getHeight()) {
        raster.setSample(rasterX, rasterY, 0, value);
      }

      // Increment by fractional steps
      x += stepX;
      y += stepY;
    }
  }

  private static RasterizationParams calculateRasterizationParams(
      Geometry geom,
      GridCoverage2D raster,
      double[] metadata,
      boolean useGeometryExtent,
      String pixelType) {

    Envelope2D rasterExtent =
        useGeometryExtent
            ? JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D())
            : JTS.getEnvelope2D(
                ReferencedEnvelope.create(
                    raster.getEnvelope(), raster.getCoordinateReferenceSystem()),
                raster.getCoordinateReferenceSystem2D());

    double scaleX = Math.abs(metadata[4]);
    double scaleY = Math.abs(metadata[5]);

    double alignedMinX = (Math.floor(rasterExtent.getMinX() / scaleX) - 1) * scaleX;
    double alignedMinY = (Math.floor(rasterExtent.getMinY() / scaleY) - 1) * scaleY;
    double alignedMaxX = (Math.ceil(rasterExtent.getMaxX() / scaleX) + 1) * scaleX;
    double alignedMaxY = (Math.ceil(rasterExtent.getMaxY() / scaleY) + 1) * scaleY;

    Envelope2D alignedRasterExtent =
        new Envelope2D(
            rasterExtent.getCoordinateReferenceSystem(),
            alignedMinX,
            alignedMinY,
            alignedMaxX - alignedMinX,
            alignedMaxY - alignedMinY);

    int rasterWidth = (int) Math.ceil((alignedMaxX - alignedMinX) / scaleX);
    int rasterHeight = (int) Math.ceil((alignedMaxY - alignedMinY) / scaleY);

    WritableRaster writableRaster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(pixelType), rasterWidth, rasterHeight, 1, null);

    System.out.println("  Raster extent min: " + alignedMinX + "," + alignedMinY + " - ");
    System.out.println("  Raster extent max: " + alignedMaxX + "," + alignedMaxY + " - ");

    System.out.println("\nupperLeftX: " + metadata[0]);
    System.out.println("upperLeftY: " + metadata[1]);
    System.out.println("rasterHeight: " + rasterHeight);
    System.out.println("rasterWidth: " + rasterWidth);

    return new RasterizationParams(writableRaster, alignedRasterExtent, scaleX, scaleY);
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

  private static class RasterizationParams {
    WritableRaster writableRaster;
    Envelope2D alignedRasterExtent;
    double scaleX;
    double scaleY;

    RasterizationParams(
        WritableRaster writableRaster,
        Envelope2D alignedRasterExtent,
        double scaleX,
        double scaleY) {
      this.writableRaster = writableRaster;
      this.alignedRasterExtent = alignedRasterExtent;
      this.scaleX = scaleX;
      this.scaleY = scaleY;
    }
  }
}
