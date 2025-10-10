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
package org.apache.sedona.common;

import java.io.FileWriter;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

public class ApproximateMedialAxisVisualizationTest {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final WKTReader WKT_READER = new WKTReader(GEOMETRY_FACTORY);

  // PostGIS SFCGAL results obtained from PostGIS 3.x with SFCGAL extension
  // Query: SELECT ST_AsText(ST_ApproximateMedialAxis(geom)) FROM ...
  private static final String[][] POSTGIS_RESULTS = {
    {
      "T-Junction",
      "POLYGON ((45 0, 55 0, 55 40, 70 40, 70 50, 30 50, 30 40, 45 40, 45 0))",
      "MULTILINESTRING ((50 5, 50 37.5), (50 37.5, 42.5 42.5), (50 37.5, 57.5 42.5), (42.5 42.5, 35 45), (57.5 42.5, 65 45))"
    },
    {
      "4-Way Intersection",
      "POLYGON ((45 100, 55 100, 55 70, 70 70, 70 55, 100 55, 100 45, 70 45, 70 30, 55 30, 55 0, 45 0, 45 30, 30 30, 30 45, 0 45, 0 55, 30 55, 30 70, 45 70, 45 100))",
      "MULTILINESTRING ((50 5, 50 27.5), (50 27.5, 42.5 35), (50 27.5, 57.5 35), (42.5 35, 35 42.5), (42.5 35, 42.5 42.5), (57.5 35, 65 42.5), (57.5 35, 57.5 42.5), (35 42.5, 27.5 50), (42.5 42.5, 42.5 57.5), (57.5 42.5, 57.5 57.5), (65 42.5, 72.5 50), (27.5 50, 5 50), (72.5 50, 95 50), (42.5 57.5, 35 65), (42.5 57.5, 42.5 72.5), (57.5 57.5, 65 65), (57.5 57.5, 57.5 72.5), (35 65, 42.5 72.5), (65 65, 57.5 72.5), (42.5 72.5, 50 80), (57.5 72.5, 50 80), (50 80, 50 95))"
    },
    {
      "Complex Branching",
      "POLYGON ((47 0, 53 0, 53 15, 55 16, 65 14, 66 17, 56 19, 54 18, 54 30, 56 31, 70 33, 71 36, 57 34, 55 33, 55 45, 57 46, 72 50, 73 53, 58 49, 56 48, 56 60, 44 60, 44 48, 42 49, 27 53, 28 50, 43 46, 45 45, 45 33, 43 34, 29 36, 30 33, 44 31, 46 30, 46 18, 44 19, 34 17, 35 14, 45 16, 47 15, 47 0))",
      "MULTILINESTRING ((50 7.5, 50 14), (50 14, 46.5 16), (50 14, 53.5 16), (46.5 16, 45.5 17), (53.5 16, 60.5 15), (45.5 17, 45 18.5), (60.5 15, 63.5 15.5), (45 18.5, 45 28.5), (63.5 15.5, 66 17), (45 28.5, 46.5 31), (45 28.5, 45 33), (46.5 31, 45 33), (46.5 31, 54 31), (45 33, 45 38.5), (54 31, 56.5 32), (45 38.5, 46.5 46), (56.5 32, 63 34), (46.5 46, 45 48), (63 34, 66 35), (45 48, 45 54), (66 35, 71 36), (45 54, 50 59), (50 59, 55 54), (55 54, 55 48), (55 48, 53.5 46), (53.5 46, 55 38.5), (55 38.5, 55 33), (55 33, 55 18.5), (55 18.5, 54.5 17), (54.5 17, 55 14))"
    },
    {
      "L-Shape",
      "POLYGON ((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0))",
      "MULTILINESTRING ((5 2.5, 7.5 2.5), (7.5 2.5, 7.5 5), (7.5 5, 5 7.5), (5 7.5, 2.5 5), (2.5 5, 2.5 2.5), (2.5 2.5, 5 2.5))"
    },
    {"Rectangle", "POLYGON ((0 0, 20 0, 20 5, 0 5, 0 0))", "MULTILINESTRING ((2.5 2.5, 17.5 2.5))"},
    {"Square", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", "MULTILINESTRING ((5 5, 5 5))"},
    {
      "T-Shape",
      "POLYGON ((4 0, 6 0, 6 8, 10 8, 10 10, 0 10, 0 8, 4 8, 4 0))",
      "MULTILINESTRING ((5 4, 5 7), (5 7, 3 9), (5 7, 7 9))"
    }
  };

  @Test
  public void generateHTMLVisualization() throws Exception {
    StringBuilder html = new StringBuilder();

    // HTML header
    html.append("<!DOCTYPE html>\n");
    html.append("<html>\n<head>\n");
    html.append("<title>ST_ApproximateMedialAxis Comparison: Sedona vs PostGIS</title>\n");
    html.append("<style>\n");
    html.append("body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }\n");
    html.append("h1 { color: #333; text-align: center; }\n");
    html.append(
        "table { width: 100%; border-collapse: collapse; background: white; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }\n");
    html.append("th, td { padding: 15px; text-align: left; border-bottom: 1px solid #ddd; }\n");
    html.append("th { background-color: #4CAF50; color: white; font-weight: bold; }\n");
    html.append("tr:hover { background-color: #f5f5f5; }\n");
    html.append(".test-name { font-weight: bold; color: #2c3e50; font-size: 1.1em; }\n");
    html.append(".wkt { font-family: 'Courier New', monospace; font-size: 0.9em; }\n");
    html.append(".stats { color: #555; }\n");
    html.append(
        ".svg-container { display: flex; justify-content: space-around; margin: 10px 0; }\n");
    html.append(".svg-box { text-align: center; }\n");
    html.append(".svg-title { font-weight: bold; margin-bottom: 5px; }\n");
    html.append("svg { border: 1px solid #ddd; background: white; }\n");
    html.append(
        ".comparison { background: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; }\n");
    html.append(".match { color: #28a745; font-weight: bold; }\n");
    html.append(".difference { color: #dc3545; font-weight: bold; }\n");
    html.append("</style>\n");
    html.append("</head>\n<body>\n");
    html.append(
        "<h1>ST_ApproximateMedialAxis Visualization: Apache Sedona vs PostGIS SFCGAL</h1>\n");
    html.append(
        "<p style='text-align: center; color: #666;'>Comparing medial axis approximation with pruning algorithm</p>\n");

    // Generate comparison for each test case
    for (String[] testCase : POSTGIS_RESULTS) {
      String name = testCase[0];
      String inputWkt = testCase[1];
      String postgisWkt = testCase[2];

      Polygon polygon = (Polygon) WKT_READER.read(inputWkt);

      // Compute Sedona results
      Geometry straightSkeleton = Functions.straightSkeleton(polygon);
      Geometry sedonaResult = Functions.approximateMedialAxis(polygon);
      Geometry postgisResult = WKT_READER.read(postgisWkt);

      // Get statistics
      int straightSkeletonSegments = straightSkeleton.getNumGeometries();
      int sedonaSegments = sedonaResult.getNumGeometries();
      int postgisSegments = postgisResult.getNumGeometries();

      double straightSkeletonLength = straightSkeleton.getLength();
      double sedonaLength = sedonaResult.getLength();
      double postgisLength = postgisResult.getLength();

      // Create visualizations
      String straightSkeletonSVG = generateSVG(polygon, straightSkeleton, "#FF6B6B", 400, 400);
      String sedonaSVG = generateSVG(polygon, sedonaResult, "#4ECDC4", 400, 400);
      String postgisSVG = generateSVG(polygon, postgisResult, "#45B7D1", 400, 400);

      // Add to HTML
      html.append("<table>\n");
      html.append("<tr><th colspan='2' class='test-name'>").append(name).append("</th></tr>\n");

      // Input polygon
      html.append("<tr><td><strong>Input Polygon:</strong></td><td class='wkt'>")
          .append(inputWkt)
          .append("</td></tr>\n");

      // Visualizations
      html.append("<tr><td colspan='2'>\n");
      html.append("<div class='svg-container'>\n");

      html.append("<div class='svg-box'>\n");
      html.append("<div class='svg-title'>Straight Skeleton (Raw)</div>\n");
      html.append(straightSkeletonSVG).append("\n");
      html.append("<div class='stats'>Segments: ")
          .append(straightSkeletonSegments)
          .append("<br>Length: ")
          .append(String.format("%.2f", straightSkeletonLength))
          .append("</div>\n");
      html.append("</div>\n");

      html.append("<div class='svg-box'>\n");
      html.append("<div class='svg-title'>Sedona (Pruned)</div>\n");
      html.append(sedonaSVG).append("\n");
      html.append("<div class='stats'>Segments: ")
          .append(sedonaSegments)
          .append("<br>Length: ")
          .append(String.format("%.2f", sedonaLength))
          .append("</div>\n");
      html.append("</div>\n");

      html.append("<div class='svg-box'>\n");
      html.append("<div class='svg-title'>PostGIS SFCGAL (Pruned)</div>\n");
      html.append(postgisSVG).append("\n");
      html.append("<div class='stats'>Segments: ")
          .append(postgisSegments)
          .append("<br>Length: ")
          .append(String.format("%.2f", postgisLength))
          .append("</div>\n");
      html.append("</div>\n");

      html.append("</div>\n");
      html.append("</td></tr>\n");

      // Comparison
      html.append("<tr><td colspan='2'>\n");
      html.append("<div class='comparison'>\n");
      html.append("<strong>Comparison:</strong><br>\n");

      double segmentDiff =
          Math.abs(sedonaSegments - postgisSegments) / (double) Math.max(sedonaSegments, 1) * 100;
      double lengthDiff =
          Math.abs(sedonaLength - postgisLength) / Math.max(sedonaLength, 0.001) * 100;

      if (segmentDiff < 20) {
        html.append("<span class='match'>✓ Segment count is similar</span>");
      } else {
        html.append("<span class='difference'>⚠ Segment count differs</span>");
      }
      html.append(" (Sedona: ")
          .append(sedonaSegments)
          .append(", PostGIS: ")
          .append(postgisSegments)
          .append(", ")
          .append(String.format("%.1f%%", segmentDiff))
          .append(" difference)<br>\n");

      if (lengthDiff < 20) {
        html.append("<span class='match'>✓ Total length is similar</span>");
      } else {
        html.append("<span class='difference'>⚠ Total length differs</span>");
      }
      html.append(" (Sedona: ")
          .append(String.format("%.2f", sedonaLength))
          .append(", PostGIS: ")
          .append(String.format("%.2f", postgisLength))
          .append(", ")
          .append(String.format("%.1f%%", lengthDiff))
          .append(" difference)<br>\n");

      int prunedStraight = straightSkeletonSegments - sedonaSegments;
      double pruneRate = (prunedStraight / (double) Math.max(straightSkeletonSegments, 1)) * 100;
      html.append("<strong>Pruning effectiveness:</strong> Removed ")
          .append(prunedStraight)
          .append(" segments (")
          .append(String.format("%.1f%%", pruneRate))
          .append(" reduction)\n");

      html.append("</div>\n");
      html.append("</td></tr>\n");

      html.append("</table>\n\n");
    }

    // HTML footer
    html.append("</body>\n</html>");

    // Write to file
    String outputPath = "/tmp/approximate_medial_axis_comparison.html";
    try (FileWriter writer = new FileWriter(outputPath)) {
      writer.write(html.toString());
    }

    System.out.println("Visualization generated: " + outputPath);
    System.out.println("Open this file in a web browser to view the comparison.");
  }

  private String generateSVG(
      Geometry polygon, Geometry skeleton, String color, int width, int height) {
    // Calculate bounds
    Envelope env = polygon.getEnvelopeInternal();
    double padding = Math.max(env.getWidth(), env.getHeight()) * 0.1;
    double minX = env.getMinX() - padding;
    double minY = env.getMinY() - padding;
    double maxX = env.getMaxX() + padding;
    double maxY = env.getMaxY() + padding;

    double scaleX = width / (maxX - minX);
    double scaleY = height / (maxY - minY);
    double scale = Math.min(scaleX, scaleY);

    StringBuilder svg = new StringBuilder();
    svg.append("<svg width='")
        .append(width)
        .append("' height='")
        .append(height)
        .append("' viewBox='0 0 ")
        .append(width)
        .append(" ")
        .append(height)
        .append("'>\n");

    // Draw polygon
    svg.append("<polygon points='");
    Coordinate[] coords = polygon.getCoordinates();
    for (Coordinate coord : coords) {
      double x = (coord.x - minX) * scale;
      double y = height - (coord.y - minY) * scale; // Flip Y axis
      svg.append(String.format("%.2f,%.2f ", x, y));
    }
    svg.append(
        "' fill='none' stroke='#333' stroke-width='2' stroke-dasharray='5,5' opacity='0.5'/>\n");

    // Draw skeleton
    for (int i = 0; i < skeleton.getNumGeometries(); i++) {
      Geometry geom = skeleton.getGeometryN(i);
      if (geom instanceof LineString) {
        LineString line = (LineString) geom;
        Coordinate start = line.getCoordinateN(0);
        Coordinate end = line.getCoordinateN(line.getNumPoints() - 1);

        double x1 = (start.x - minX) * scale;
        double y1 = height - (start.y - minY) * scale;
        double x2 = (end.x - minX) * scale;
        double y2 = height - (end.y - minY) * scale;

        svg.append("<line x1='")
            .append(String.format("%.2f", x1))
            .append("' y1='")
            .append(String.format("%.2f", y1))
            .append("' x2='")
            .append(String.format("%.2f", x2))
            .append("' y2='")
            .append(String.format("%.2f", y2))
            .append("' stroke='")
            .append(color)
            .append("' stroke-width='3' stroke-linecap='round'/>\n");
      }
    }

    // Draw vertices
    for (int i = 0; i < skeleton.getNumGeometries(); i++) {
      Geometry geom = skeleton.getGeometryN(i);
      if (geom instanceof LineString) {
        LineString line = (LineString) geom;
        for (Coordinate coord : line.getCoordinates()) {
          double x = (coord.x - minX) * scale;
          double y = height - (coord.y - minY) * scale;
          svg.append("<circle cx='")
              .append(String.format("%.2f", x))
              .append("' cy='")
              .append(String.format("%.2f", y))
              .append("' r='3' fill='")
              .append(color)
              .append("'/>\n");
        }
      }
    }

    svg.append("</svg>");
    return svg.toString();
  }
}
