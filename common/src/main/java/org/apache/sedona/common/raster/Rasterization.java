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
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.*;
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

    System.out.println("Custom Rasterization");
    System.out.println("allTouched = " + allTouched);

    // Validate the input geometry and raster metadata
    double[] metadata = RasterAccessors.metadata(raster);
    validateRasterMetadata(metadata);

    Envelope2D rasterExtent = raster.getEnvelope2D();
    Envelope2D geomExtent = rasterizeGeomExtent(geom, raster, metadata, allTouched);

    RasterizationParams params =
        calculateRasterizationParams(
            raster, useGeometryExtent, allTouched, rasterExtent, metadata, geomExtent, pixelType);

    rasterizeGeometry(geom, params, geomExtent, value, allTouched);

    // Create a GridCoverage2D for the rasterized result
    GridCoverageFactory coverageFactory = new GridCoverageFactory();
    GridCoverage2D rasterized;

    if (useGeometryExtent) {
      rasterized = coverageFactory.create("rasterized", params.writableRaster, geomExtent);
    } else {
      rasterized = coverageFactory.create("rasterized", params.writableRaster, rasterExtent);
    }

    // Return results compatible with the original function
    List<Object> objects = new ArrayList<>();
    objects.add(params.writableRaster);
    objects.add(rasterized);

    return objects;
  }

  private static void rasterizeGeometry(
      Geometry geom,
      RasterizationParams params,
      Envelope2D geomExtent,
      double value,
      boolean allTouched)
      throws FactoryException {

    switch (geom.getGeometryType()) {
      case "GeometryCollection":
        //        rasterizeGeometryCollection(
        //            geom, params, value, true, params.useGeometryExtent, upperLeftX, upperLeftY);
      case "Point":
      case "MultiPoint":
        System.out.println("rasterizeGeometry for Point and MulitPoint...");
        rasterizePolygon(geom, params, geomExtent, value, true);
        break;
      case "LineString":
      case "MultiLineString":
      case "LinearRing":
        rasterizeLineString(geom, params, geomExtent, value);
        break;
      default:
        rasterizePolygon(geom, params, geomExtent, value, allTouched);
        break;
    }
  }

  //  private static void rasterizeGeometryCollection(
  //      Geometry geom,
  //      RasterizationParams globalParams,
  //      double value,
  //      boolean allTouched,
  //      boolean useGeometryExtent,
  //      double upperLeftX,
  //      double upperLeftY)
  //      throws FactoryException {
  //    // Validate the input geometry and raster metadata
  //    double[] metadata = RasterAccessors.metadata(globalParams.originalRaster);
  //    for (int i = 0; i < geom.getNumGeometries(); i++) {
  //      Geometry subGeom = geom.getGeometryN(i);
  //      System.out.println("\nWorking on " + Functions.asEWKT(subGeom));
  //
  //      // Define and align raster grid properties
  //      RasterizationParams params =
  //          calculateRasterizationParams(
  //              subGeom,
  //              globalParams.originalRaster,
  //              allTouched,
  //              metadata,
  //              useGeometryExtent,
  //              globalParams.pixelType);
  //      params.writableRaster = globalParams.writableRaster;
  //
  //      //      System.out.println("alignedRasterExtent: " + geomExtent.toString());
  //
  //      rasterizeGeometry(
  //          subGeom, params, value, allTouched, useGeometryExtent, upperLeftX, upperLeftY);
  //
  //      GridCoverageFactory cf = new GridCoverageFactory();
  //      GridCoverage2D test = cf.create("raster", params.writableRaster, geomExtent);
  //      //      System.out.println(
  //      //          "Rasterized subGeom metadata: " +
  //      // Arrays.toString(RasterAccessors.metadata(test)));
  //      //      System.out.println(
  //      //          "Rasterized subGeom band 1: " + Arrays.toString(MapAlgebra.bandAsArray(test,
  // 1)));
  //    }
  //  }

  private static void rasterizePolygon(
      Geometry geom,
      RasterizationParams params,
      Envelope2D geomExtent,
      double value,
      boolean allTouched) {
    System.out.println("\nCalling rasterizePolygon...");
    int rasterWidth = params.writableRaster.getWidth();
    int rasterHeight = params.writableRaster.getHeight();

    System.out.println("writableRaster width and height: " + rasterWidth + ", " + rasterHeight);
    //    System.out.println("rasterWidth, rasterHeight: " + rasterWidth + ", " + rasterHeight);
    double dx = Math.round(((geomExtent.getMinX() - params.upperLeftX) / params.scaleX));
    double dy = Math.round(((geomExtent.getMinY() - params.upperLeftY) / params.scaleY));
    int loopCount = 0;

    //    System.out.println("Original raster Envelope: " + params.originalRaster.getEnvelope2D());
    System.out.println("alignedRasterExtent Envelope: " + geomExtent);
    System.out.println("worldX range: " + geomExtent.getMinX() + " - " + geomExtent.getMaxX());
    System.out.println("worldY range: " + geomExtent.getMinY() + " - " + geomExtent.getMaxY());
    System.out.println("upperLeftX, upperLeftY: " + params.upperLeftX + ", " + params.upperLeftY);
    System.out.println("dx, dy: " + dx + ", " + dy);

    for (double worldY = geomExtent.getMinY();
        worldY < geomExtent.getMaxY();
        worldY += params.scaleY) {
      for (double worldX = geomExtent.getMinX();
          worldX < geomExtent.getMaxX();
          worldX += params.scaleX) {
        loopCount += 1;

        // Compute corresponding raster indices
        int x = (int) Math.round((worldX - geomExtent.getMinX()) / params.scaleX);
        int y = (int) Math.round((worldY - geomExtent.getMinY()) / params.scaleY);
        x += (int) dx;
        y += (int) dy;

        // Flip y-axis (since raster Y starts from top-left)
        int yIndex = -y - 1;

        //        System.out.println("\nworldX, worldY: " + worldX + ", " + worldY);
        //        System.out.println("x, y: " + x + ", " + y);
        //        System.out.println("x, yIndex: " + x + ", " + yIndex);

        // Create envelope for this pixel
        double cellMinX = worldX;
        double cellMaxX = cellMinX + params.scaleX;
        double cellMinY = worldY;
        double cellMaxY = cellMinY + params.scaleY;

        Envelope cellEnvelope = new Envelope(cellMinX, cellMaxX, cellMinY, cellMaxY);
        Geometry cellGeometry = JTS.toGeometry(cellEnvelope);

        boolean intersects =
            allTouched
                ? geom.intersects(cellGeometry)
                : geom.intersects(cellGeometry.getCentroid());

        if (intersects) {
          params.writableRaster.setSample(x, yIndex, 0, value);
        }
      }
    }

    //    System.out.println("\nwritableRaster after polyfill:");
    //    printWritableRasterWithCoordinates(params.writableRaster);
    System.out.println("loopCount: " + loopCount);
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
      Geometry geom, RasterizationParams params, Envelope2D geomExtent, double value) {

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      LineString line = (LineString) geom.getGeometryN(i);
      Coordinate[] coords = line.getCoordinates();

      for (int j = 0; j < coords.length - 1; j++) {
        // Extract start and end points for the segment
        Coordinate start = coords[j];
        Coordinate end = coords[j + 1];

        double x0 = (start.x - geomExtent.getMinX()) / params.scaleX;
        double y0 = (geomExtent.getMaxY() - start.y) / params.scaleY;
        double x1 = (end.x - geomExtent.getMinX()) / params.scaleX;
        double y1 = (geomExtent.getMaxY() - end.y) / params.scaleY;

        // Debug information
        //        System.out.printf(
        //            "Rasterizing segment: Start (%6.2f, %6.2f), End (%6.2f, %6.2f)%n", x0, y0, x1,
        // y1);

        // Apply Bresenham for this segment
        drawLineBresenham(params.writableRaster, x0, y0, x1, y1, value, 0.5);
      }
    }

    //    System.out.println("\nWritable Raster after rasterizing LineString:");
    //    printWritableRasterWithCoordinates(writableRaster);
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

  private static Envelope2D rasterizeGeomExtent(
      Geometry geom, GridCoverage2D raster, double[] metadata, boolean allTouched) {

    Envelope2D rasterExtent =
        JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());

    // Extract scale values
    double scaleX = Math.abs(metadata[4]);
    double scaleY = Math.abs(metadata[5]);

    // Compute the aligned min/max values
    double alignedMinX = Math.floor(rasterExtent.getMinX() / scaleX) * scaleX;
    double alignedMinY = Math.floor(rasterExtent.getMinY() / scaleY) * scaleY;
    double alignedMaxX = Math.ceil(rasterExtent.getMaxX() / scaleX) * scaleX;
    double alignedMaxY = Math.ceil(rasterExtent.getMaxY() / scaleY) * scaleY;

    // Get the extent of the original raster
    double originalMinX = raster.getEnvelope().getMinimum(0);
    double originalMinY = raster.getEnvelope().getMinimum(1);
    double originalMaxX = raster.getEnvelope().getMaximum(0);
    double originalMaxY = raster.getEnvelope().getMaximum(1);

    // Extend the aligned extent by 1 pixel if allTouched is true,
    // while keeping within the original raster bounds
    if (allTouched) {
      alignedMinX = alignedMinX - scaleX;
      alignedMinY = alignedMinY - scaleY;
      alignedMaxX = alignedMaxX + scaleX;
      alignedMaxY = alignedMaxY + scaleY;
    }

    alignedMinX = Math.max(alignedMinX, originalMinX);
    alignedMinY = Math.max(alignedMinY, originalMinY);
    alignedMaxX = Math.min(alignedMaxX, originalMaxX);
    alignedMaxY = Math.min(alignedMaxY, originalMaxY);

    // For points at intersection of 2 or more pixels,
    // extend search grid by 1 pixel in each direction
    if (alignedMaxX == alignedMinX) {
      System.out.println("\nFound alignedMaxX == alignedMinX ...");
      alignedMinX -= scaleX;
      alignedMaxX += scaleX;
    }
    if (alignedMaxY == alignedMinY) {
      System.out.println("\nFound alignedMaxY == alignedMinY ...");
      alignedMinY -= scaleY;
      alignedMaxY += scaleY;
    }

    int alignedRasterWidth, alignedRasterHeight;

    alignedRasterWidth = (int) Math.ceil((alignedMaxX - alignedMinX) / scaleX);
    alignedRasterHeight = (int) Math.ceil((alignedMaxY - alignedMinY) / scaleY);

    System.out.println(
        "\nalignedRasterWidth, alignedRasterHeight: "
            + alignedRasterWidth
            + ", "
            + alignedRasterHeight);

    // Create the aligned raster extent
    Envelope2D alignedRasterExtent =
        new Envelope2D(
            rasterExtent.getCoordinateReferenceSystem(),
            alignedMinX,
            alignedMinY,
            alignedMaxX - alignedMinX,
            alignedMaxY - alignedMinY);

    return alignedRasterExtent;
  }

  private static RasterizationParams calculateRasterizationParams(
      GridCoverage2D raster,
      boolean useGeometryExtent,
      boolean allTouched,
      Envelope2D rasterExtent,
      double[] metadata,
      Envelope2D geomExtent,
      String pixelType) {

    double scaleX = Math.abs(metadata[4]);
    double scaleY = Math.abs(metadata[5]);

    double upperLeftX = 0;
    double upperLeftY = 0;
    if (useGeometryExtent) {
      upperLeftX = geomExtent.getMinX();
      upperLeftY = geomExtent.getMaxY();
    } else {
      upperLeftX = metadata[0];
      upperLeftY = metadata[1];
    }

    WritableRaster writableRaster;
    if (useGeometryExtent) {
      int geomExtentWidth = (int) (Math.ceil(geomExtent.getWidth() / scaleX));
      int geomExtentHeight = (int) (Math.ceil(geomExtent.getHeight() / scaleY));
      System.out.println(
          "\ngeomExtentWidth, geomExtentHeight: " + geomExtentWidth + ", " + geomExtentHeight);

      writableRaster =
          RasterFactory.createBandedRaster(
              RasterUtils.getDataTypeCode(pixelType), geomExtentWidth, geomExtentHeight, 1, null);
    } else {
      int rasterWidth = (int) raster.getGridGeometry().getGridRange2D().getWidth();
      int rasterHeight = (int) raster.getGridGeometry().getGridRange2D().getHeight();
      writableRaster =
          RasterFactory.createBandedRaster(
              RasterUtils.getDataTypeCode(pixelType), rasterWidth, rasterHeight, 1, null);
    }

    return new RasterizationParams(
        writableRaster, allTouched, pixelType, scaleX, scaleY, upperLeftX, upperLeftY);
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

  // New condensed Rasterization parameters
  private static class RasterizationParams {
    WritableRaster writableRaster;
    boolean allTouched;
    String pixelType;
    double scaleX;
    double scaleY;
    double upperLeftX;
    double upperLeftY;

    RasterizationParams(
        WritableRaster writableRaster,
        boolean allTouched,
        String pixelType,
        double scaleX,
        double scaleY,
        double upperLeftX,
        double upperLeftY) {
      this.writableRaster = writableRaster;
      this.allTouched = allTouched;
      this.pixelType = pixelType;
      this.scaleX = scaleX;
      this.scaleY = scaleY;
      this.upperLeftX = upperLeftX;
      this.upperLeftY = upperLeftY;
    }
  }
}
