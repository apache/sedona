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
import java.util.Objects;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.predicate.RectangleIntersects;
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

    // Validate the input geometry and raster metadata
    double[] metadata = RasterAccessors.metadata(raster);
    validateRasterMetadata(metadata);
    if (!RasterPredicates.rsIntersects(raster, geom)) {
      throw new IllegalArgumentException("Geometry does not intersect Raster.");
    }

    Envelope2D rasterExtent = raster.getEnvelope2D();

    Envelope2D geomExtent = rasterizeGeomExtent(geom, raster, metadata, allTouched);

    RasterizationParams params =
        calculateRasterizationParams(raster, useGeometryExtent, metadata, geomExtent, pixelType);

    rasterizeGeometry(raster, metadata, geom, params, geomExtent, value, allTouched);

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
      GridCoverage2D raster,
      double[] metadata,
      Geometry geom,
      RasterizationParams params,
      Envelope2D geomExtent,
      double value,
      boolean allTouched)
      throws FactoryException {

    switch (geom.getGeometryType()) {
      case "GeometryCollection":
      case "MultiPolygon":
      case "MultiPoint":
        rasterizeGeometryCollection(raster, metadata, geom, params, value, allTouched);
        break;
      case "Point":
        rasterizePolygon(geom, params, geomExtent, value, true);
        break;
      case "LineString":
      case "MultiLineString":
      case "LinearRing":
        rasterizeLineString(geom, params, value);
        break;
      default:
        rasterizePolygon(geom, params, geomExtent, value, allTouched);
        break;
    }
  }

  private static void rasterizeGeometryCollection(
      GridCoverage2D raster,
      double[] metadata,
      Geometry geom,
      RasterizationParams params,
      double value,
      boolean allTouched)
      throws FactoryException {

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry subGeom = geom.getGeometryN(i);
      Envelope2D subGeomExtent = rasterizeGeomExtent(subGeom, raster, metadata, allTouched);
      rasterizeGeometry(raster, metadata, subGeom, params, subGeomExtent, value, allTouched);
    }
  }

  private static void rasterizePolygon(
      Geometry geom,
      RasterizationParams params,
      Envelope2D geomExtent,
      double value,
      boolean allTouched) {

    //    double dx = Math.round(((geomExtent.getMinX() - params.upperLeftX) / params.scaleX));
    //    double dy = Math.round(((geomExtent.getMinY() - params.upperLeftY) / params.scaleY));

    int dx = (int) Math.round(((geomExtent.getMinX() - params.upperLeftX) / params.scaleX));
    int dy = (int) Math.round(((geomExtent.getMinY() - params.upperLeftY) / params.scaleY));
    int x = dx;
    int y = dy;

    // Compute initial centroid values outside the loop
    //    double startCentroidX = geomExtent.getMinX() + (params.scaleX * 0.5);
    //    double startCentroidY = geomExtent.getMinY() + (params.scaleY * 0.5);
    //    double centroidX = startCentroidX;
    //    double centroidY = startCentroidY;

    //    GeometryFactory factory = new GeometryFactory();

    for (double worldY = geomExtent.getMinY();
        worldY < geomExtent.getMaxY();
        worldY += params.scaleY, y++) {
      x = dx;
      //      centroidX = startCentroidX;
      for (double worldX = geomExtent.getMinX();
          worldX < geomExtent.getMaxX();
          worldX += params.scaleX, x++) {

        // Compute corresponding raster indices
        //        int x = (int) Math.round((worldX - geomExtent.getMinX()) / params.scaleX);
        //        int y = (int) Math.round((worldY - geomExtent.getMinY()) / params.scaleY);
        //        x += (int) dx;
        //        y += (int) dy;

        //        System.out.println("\nworldX, worldY: " + worldX + ", " + worldY);
        //        System.out.println("x, y: " + x + ", " + y);
        //        x += 1;
        //        y += 1;

        // Flip y-axis (since raster Y starts from top-left)
        int yIndex = -y - 1;

        // Create envelope for this pixel
        double cellMaxX = worldX + params.scaleX;
        double cellMaxY = worldY + params.scaleY;

        //                Envelope cellEnvelope = new Envelope(worldX, cellMaxX, worldY, cellMaxY);
        //                Polygon cellPolygon = JTS.toGeometry(cellEnvelope);
        // Compute centroid directly
        //        double centroidX = (worldX + cellMaxX) * 0.5;
        //        double centroidY = (worldY + cellMaxY) * 0.5;

        Polygon cellPolygon = JTS.toGeometry(new Envelope(worldX, cellMaxX, worldY, cellMaxY));

        boolean intersects =
            allTouched
                ? RectangleIntersects.intersects(cellPolygon, geom)
                : geom.intersects(cellPolygon.getCentroid());
        //        boolean intersects =
        //            allTouched
        //                ? geom.intersects(JTS.toGeometry(new Envelope(worldX, cellMaxX, worldY,
        // cellMaxY)))
        //                : geom.intersects(factory.createPoint(new Coordinate(centroidX,
        // centroidY)));

        if (intersects) {
          params.writableRaster.setSample(x, yIndex, 0, value);
        }
      }
      //      centroidY += params.scaleY;
    }
  }

  private static void rasterizeLineString(Geometry geom, RasterizationParams params, double value) {
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      LineString line = (LineString) geom.getGeometryN(i);
      Coordinate[] coords = line.getCoordinates();

      for (int j = 0; j < coords.length - 1; j++) {
        // Extract start and end points for the segment
        Coordinate start = coords[j];
        Coordinate end = coords[j + 1];

        double x0 = (start.x - params.upperLeftX) / params.scaleX;
        double y0 = (params.upperLeftY - start.y) / params.scaleY;
        double x1 = (end.x - params.upperLeftX) / params.scaleX;
        double y1 = (params.upperLeftY - end.y) / params.scaleY;

        // Apply Bresenham for this segment
        drawLineBresenham(params, x0, y0, x1, y1, value, 0.5);
      }
    }
  }

  // Modified Bresenham with Fractional Steps
  private static void drawLineBresenham(
      RasterizationParams params,
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
      int rasterX = (int) (Math.floor(x));
      int rasterY = (int) (Math.floor(y));

      // Only write if within raster bounds
      if (rasterX >= 0
          && rasterX < params.writableRaster.getWidth()
          && rasterY >= 0
          && rasterY < params.writableRaster.getHeight()) {
        params.writableRaster.setSample(rasterX, rasterY, 0, value);
      }

      // Increment by fractional steps
      x += stepX;
      y += stepY;
    }
  }

  private static Envelope2D rasterizeGeomExtent(
      Geometry geom, GridCoverage2D raster, double[] metadata, boolean allTouched) {

    if (Objects.equals(geom.getGeometryType(), "MultiLineString")) {
      allTouched = true;
    }
    if (Objects.equals(geom.getGeometryType(), "MultiPoint")) {
      allTouched = true;
    }

    Envelope2D rasterExtent =
        JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());

    // Compute the aligned min/max values
    double alignedMinX =
        (Math.floor((rasterExtent.getMinX() + metadata[0]) / metadata[4]) * metadata[4])
            - metadata[0];
    double alignedMinY =
        (Math.floor((rasterExtent.getMinY() + metadata[1]) / -metadata[5]) * -metadata[5])
            - metadata[1];
    double alignedMaxX =
        (Math.ceil((rasterExtent.getMaxX() + metadata[0]) / metadata[4]) * metadata[4])
            - metadata[0];
    double alignedMaxY =
        (Math.ceil((rasterExtent.getMaxY() + metadata[1]) / -metadata[5]) * -metadata[5])
            - metadata[1];

    // For points and LineStrings at intersection of 2 or more pixels,
    // extend search grid by 1 pixel in each direction
    if (alignedMaxX == alignedMinX) {
      alignedMinX -= metadata[4];
      alignedMaxX += metadata[4];
    }
    if (alignedMaxY == alignedMinY) {
      alignedMinY += metadata[5];
      alignedMaxY -= metadata[5];
    }

    // Get the extent of the original raster
    double originalMinX = raster.getEnvelope().getMinimum(0);
    double originalMinY = raster.getEnvelope().getMinimum(1);
    double originalMaxX = raster.getEnvelope().getMaximum(0);
    double originalMaxY = raster.getEnvelope().getMaximum(1);

    // Extend the aligned extent by 1 pixel if allTouched is true and if any geometry extent meets
    // the aligned extent
    if (allTouched) {
      alignedMinX -= (rasterExtent.getMinX() == alignedMinX) ? metadata[4] : 0;
      alignedMinY += (rasterExtent.getMinY() == alignedMinY) ? metadata[5] : 0;
      alignedMaxX += (rasterExtent.getMaxX() == alignedMaxX) ? metadata[4] : 0;
      alignedMaxY -= (rasterExtent.getMaxY() == alignedMaxY) ? metadata[5] : 0;
    }

    alignedMinX = Math.max(alignedMinX, originalMinX);
    alignedMinY = Math.max(alignedMinY, originalMinY);
    alignedMaxX = Math.min(alignedMaxX, originalMaxX);
    alignedMaxY = Math.min(alignedMaxY, originalMaxY);

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
      double[] metadata,
      Envelope2D geomExtent,
      String pixelType) {

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
      int geomExtentWidth = (int) (Math.round(geomExtent.getWidth() / metadata[4]));
      int geomExtentHeight = (int) (Math.round(geomExtent.getHeight() / -metadata[5]));

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
        writableRaster, pixelType, metadata[4], -metadata[5], upperLeftX, upperLeftY);
  }

  private static void validateRasterMetadata(double[] rasterMetadata) {
    if (rasterMetadata[4] < 0) {
      throw new IllegalArgumentException("ScaleX cannot be negative");
    }
    if (rasterMetadata[5] > 0) {
      throw new IllegalArgumentException("ScaleY must be negative");
    }
    if (rasterMetadata[6] != 0 || rasterMetadata[7] != 0) {
      throw new IllegalArgumentException("SkewX and SkewY must be zero");
    }
  }

  // New condensed Rasterization parameters
  private static class RasterizationParams {
    WritableRaster writableRaster;
    String pixelType;
    double scaleX;
    double scaleY;
    double upperLeftX;
    double upperLeftY;

    RasterizationParams(
        WritableRaster writableRaster,
        String pixelType,
        double scaleX,
        double scaleY,
        double upperLeftX,
        double upperLeftY) {
      this.writableRaster = writableRaster;
      this.pixelType = pixelType;
      this.scaleX = scaleX;
      this.scaleY = scaleY;
      this.upperLeftX = upperLeftX;
      this.upperLeftY = upperLeftY;
    }
  }
}
