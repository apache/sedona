/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.vividsolutions.jts.io;

import com.vividsolutions.jts.geom.*;

import com.vividsolutions.jts.util.*;
import java.io.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

/**
 * Writes the Well-Known Text representation of a {@link Geometry}.
 * The Well-Known Text format is defined in the
 * OGC <A HREF="http://www.opengis.org/techno/specs.htm">
 * <i>Simple Features Specification for SQL</i></A>.
 * See {@link WKTReader} for a formal specification of the format syntax.
 * <p>
 * The <code>WKTWriter</code> outputs coordinates rounded to the precision
 * model. Only the maximum number of decimal places 
 * necessary to represent the ordinates to the required precision will be
 * output.
 * <p>
 * The SFS WKT spec does not define a special tag for {@link LinearRing}s.
 * Under the spec, rings are output as <code>LINESTRING</code>s.
 * In order to allow precisely specifying constructed geometries, 
 * JTS also supports a non-standard <code>LINEARRING</code> tag which is used 
 * to output LinearRings.
 *
 * @version 1.7
 * @see WKTReader
 */
public class WKTWriter
{
  /**
   * Generates the WKT for a <tt>POINT</tt>
   * specified by a {@link Coordinate}.
   *
   * @param p0 the point coordinate
   *
   * @return the WKT
   */
  public static String toPoint(Coordinate p0)
  {
    return "POINT ( " + p0.x + " " + p0.y  + " )";
  }

  /**
   * Generates the WKT for a <tt>LINESTRING</tt>
   * specified by a {@link CoordinateSequence}.
   *
   * @param seq the sequence to write
   *
   * @return the WKT string
   */
  public static String toLineString(CoordinateSequence seq)
  {
    StringBuffer buf = new StringBuffer();
    buf.append("LINESTRING ");
    if (seq.size() == 0)
      buf.append(" EMPTY");
    else {
      buf.append("(");
      for (int i = 0; i < seq.size(); i++) {
        if (i > 0)
          buf.append(", ");
        buf.append(seq.getX(i) + " " + seq.getY(i));
      }
      buf.append(")");
    }
    return buf.toString();
  }
  
  /**
   * Generates the WKT for a <tt>LINESTRING</tt>
   * specified by a {@link CoordinateSequence}.
   *
   * @param seq the sequence to write
   *
   * @return the WKT string
   */
  public static String toLineString(Coordinate[] coord)
  {
    StringBuffer buf = new StringBuffer();
    buf.append("LINESTRING ");
    if (coord.length == 0)
      buf.append(" EMPTY");
    else {
      buf.append("(");
      for (int i = 0; i < coord.length; i++) {
        if (i > 0)
          buf.append(", ");
        buf.append(coord[i].x + " " + coord[i].y );
      }
      buf.append(")");
    }
    return buf.toString();
  }

  /**
   * Generates the WKT for a <tt>LINESTRING</tt>
   * specified by two {@link Coordinate}s.
   *
   * @param p0 the first coordinate
   * @param p1 the second coordinate
   *
   * @return the WKT
   */
  public static String toLineString(Coordinate p0, Coordinate p1)
  {
    return "LINESTRING ( " + p0.x + " " + p0.y + ", " + p1.x + " " + p1.y + " )";
  }

  private static final int INDENT = 2;

  /**
   *  Creates the <code>DecimalFormat</code> used to write <code>double</code>s
   *  with a sufficient number of decimal places.
   *
   *@param  precisionModel  the <code>PrecisionModel</code> used to determine
   *      the number of decimal places to write.
   *@return                 a <code>DecimalFormat</code> that write <code>double</code>
   *      s without scientific notation.
   */
  private static DecimalFormat createFormatter(PrecisionModel precisionModel) {
    // the default number of decimal places is 16, which is sufficient
    // to accomodate the maximum precision of a double.
    int decimalPlaces = precisionModel.getMaximumSignificantDigits();
    // specify decimal separator explicitly to avoid problems in other locales
    DecimalFormatSymbols symbols = new DecimalFormatSymbols();
    symbols.setDecimalSeparator('.');
    String fmtString = "0" + (decimalPlaces > 0 ? "." : "")
                 +  stringOfChar('#', decimalPlaces);
    return new DecimalFormat(fmtString, symbols);
  }

  /**
   *  Returns a <code>String</code> of repeated characters.
   *
   *@param  ch     the character to repeat
   *@param  count  the number of times to repeat the character
   *@return        a <code>String</code> of characters
   */
  public static String stringOfChar(char ch, int count) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < count; i++) {
      buf.append(ch);
    }
    return buf.toString();
  }

  private int outputDimension = 2;
  private DecimalFormat formatter;
  private boolean isFormatted = false;
  private boolean useFormatting = false;
  private int level = 0;
  private int coordsPerLine = -1;
  private String indentTabStr = "  ";

  /**
   * Creates a new WKTWriter with default settings
   */
  public WKTWriter()
  {
  }

  /**
   * Creates a writer that writes {@link Geometry}s with
   * the given output dimension (2 or 3).
   * If the specified output dimension is 3, the Z value
   * of coordinates will be written if it is present
   * (i.e. if it is not <code>Double.NaN</code>).
   *
   * @param outputDimension the coordinate dimension to output (2 or 3)
   */
  public WKTWriter(int outputDimension) {
    this.outputDimension = outputDimension;

    if (outputDimension < 2 || outputDimension > 3)
      throw new IllegalArgumentException("Invalid output dimension (must be 2 or 3)");
  }

  /**
   * Sets whether the output will be formatted.
   *
   * @param isFormatted true if the output is to be formatted
   */
  public void setFormatted(boolean isFormatted)
  {
    this.isFormatted = isFormatted;
  }

  /**
   * Sets the maximum number of coordinates per line
   * written in formatted output.
   * If the provided coordinate number is <= 0,
   * coordinates will be written all on one line.
   *
   * @param coordsPerLine the number of coordinates per line to output.
   */
  public void setMaxCoordinatesPerLine(int coordsPerLine)
  {
    this.coordsPerLine = coordsPerLine;
  }

  /**
   * Sets the tab size to use for indenting.
   *
   * @param size the number of spaces to use as the tab string
   * @throws IllegalArgumentException if the size is non-positive
   */
  public void setTab(int size)
  {
    if(size <= 0)
      throw new IllegalArgumentException("Tab count must be positive");
    this.indentTabStr = stringOfChar(' ', size);
  }

  /**
   *  Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   *@param  geometry  a <code>Geometry</code> to process
   *@return           a <Geometry Tagged Text> string (see the OpenGIS Simple
   *      Features Specification)
   */
  public String write(Geometry geometry)
  {
    Writer sw = new StringWriter();
    try {
      writeFormatted(geometry, isFormatted, sw);
    }
    catch (IOException ex) {
      Assert.shouldNeverReachHere();
    }
    return sw.toString();
  }

  /**
   *  Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   *@param  geometry  a <code>Geometry</code> to process
   */
  public void write(Geometry geometry, Writer writer)
    throws IOException
  {
    writeFormatted(geometry, false, writer);
  }

  /**
   *  Same as <code>write</code>, but with newlines and spaces to make the
   *  well-known text more readable.
   *
   *@param  geometry  a <code>Geometry</code> to process
   *@return           a <Geometry Tagged Text> string (see the OpenGIS Simple
   *      Features Specification), with newlines and spaces
   */
  public String writeFormatted(Geometry geometry)
  {
    Writer sw = new StringWriter();
    try {
      writeFormatted(geometry, true, sw);
    }
    catch (IOException ex) {
      Assert.shouldNeverReachHere();
    }
    return sw.toString();
  }
  /**
   *  Same as <code>write</code>, but with newlines and spaces to make the
   *  well-known text more readable.
   *
   *@param  geometry  a <code>Geometry</code> to process
   */
  public void writeFormatted(Geometry geometry, Writer writer)
    throws IOException
  {
    writeFormatted(geometry, true, writer);
  }
  /**
   *  Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   *@param  geometry  a <code>Geometry</code> to process
   */
  private void writeFormatted(Geometry geometry, boolean useFormatting, Writer writer)
    throws IOException
  {
    this.useFormatting = useFormatting;
    formatter = createFormatter(geometry.getPrecisionModel());
    appendGeometryTaggedText(geometry, 0, writer);
  }


  /**
   *  Converts a <code>Geometry</code> to &lt;Geometry Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  geometry  the <code>Geometry</code> to process
   *@param  writer    the output writer to append to
   */
  private void appendGeometryTaggedText(Geometry geometry, int level, Writer writer)
    throws IOException
  {
    indent(level, writer);

    if (geometry instanceof Point) {
      Point point = (Point) geometry;
      appendPointTaggedText(point.getCoordinate(), level, writer, point.getPrecisionModel());
    }
    else if (geometry instanceof LinearRing) {
      appendLinearRingTaggedText((LinearRing) geometry, level, writer);
    }
    else if (geometry instanceof LineString) {
      appendLineStringTaggedText((LineString) geometry, level, writer);
    }
    else if (geometry instanceof Polygon) {
      appendPolygonTaggedText((Polygon) geometry, level, writer);
    }
    else if (geometry instanceof MultiPoint) {
      appendMultiPointTaggedText((MultiPoint) geometry, level, writer);
    }
    else if (geometry instanceof MultiLineString) {
      appendMultiLineStringTaggedText((MultiLineString) geometry, level, writer);
    }
    else if (geometry instanceof MultiPolygon) {
      appendMultiPolygonTaggedText((MultiPolygon) geometry, level, writer);
    }
    else if (geometry instanceof GeometryCollection) {
      appendGeometryCollectionTaggedText((GeometryCollection) geometry, level, writer);
    }
    else {
      Assert.shouldNeverReachHere("Unsupported Geometry implementation:"
           + geometry.getClass());
    }
  }

  /**
   *  Converts a <code>Coordinate</code> to &lt;Point Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  coordinate      the <code>Coordinate</code> to process
   *@param  writer          the output writer to append to
   *@param  precisionModel  the <code>PrecisionModel</code> to use to convert
   *      from a precise coordinate to an external coordinate
   */
  private void appendPointTaggedText(Coordinate coordinate, int level, Writer writer,
      PrecisionModel precisionModel)
    throws IOException
  {
    writer.write("POINT ");
    appendPointText(coordinate, level, writer, precisionModel);
  }

  /**
   *  Converts a <code>LineString</code> to &lt;LineString Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  lineString  the <code>LineString</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendLineStringTaggedText(LineString lineString, int level, Writer writer)
    throws IOException
  {
    writer.write("LINESTRING ");
    appendLineStringText(lineString, level, false, writer);
  }

  /**
   *  Converts a <code>LinearRing</code> to &lt;LinearRing Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  linearRing  the <code>LinearRing</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendLinearRingTaggedText(LinearRing linearRing, int level, Writer writer)
    throws IOException
  {
    writer.write("LINEARRING ");
    appendLineStringText(linearRing, level, false, writer);
  }

  /**
   *  Converts a <code>Polygon</code> to &lt;Polygon Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  polygon  the <code>Polygon</code> to process
   *@param  writer   the output writer to append to
   */
  private void appendPolygonTaggedText(Polygon polygon, int level, Writer writer)
    throws IOException
  {
    writer.write("POLYGON ");
    appendPolygonText(polygon, level, false, writer);
  }

  /**
   *  Converts a <code>MultiPoint</code> to &lt;MultiPoint Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multipoint  the <code>MultiPoint</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendMultiPointTaggedText(MultiPoint multipoint, int level, Writer writer)
    throws IOException
  {
    writer.write("MULTIPOINT ");
    appendMultiPointText(multipoint, level, writer);
  }

  /**
   *  Converts a <code>MultiLineString</code> to &lt;MultiLineString Tagged
   *  Text&gt; format, then appends it to the writer.
   *
   *@param  multiLineString  the <code>MultiLineString</code> to process
   *@param  writer           the output writer to append to
   */
  private void appendMultiLineStringTaggedText(MultiLineString multiLineString, int level,
      Writer writer)
    throws IOException
  {
    writer.write("MULTILINESTRING ");
    appendMultiLineStringText(multiLineString, level, false, writer);
  }

  /**
   *  Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multiPolygon  the <code>MultiPolygon</code> to process
   *@param  writer        the output writer to append to
   */
  private void appendMultiPolygonTaggedText(MultiPolygon multiPolygon, int level, Writer writer)
    throws IOException
  {
    writer.write("MULTIPOLYGON ");
    appendMultiPolygonText(multiPolygon, level, writer);
  }

  /**
   *  Converts a <code>GeometryCollection</code> to &lt;GeometryCollection
   *  Tagged Text&gt; format, then appends it to the writer.
   *
   *@param  geometryCollection  the <code>GeometryCollection</code> to process
   *@param  writer              the output writer to append to
   */
  private void appendGeometryCollectionTaggedText(GeometryCollection geometryCollection, int level,
      Writer writer)
    throws IOException
  {
    writer.write("GEOMETRYCOLLECTION ");
    appendGeometryCollectionText(geometryCollection, level, writer);
  }

  /**
   *  Converts a <code>Coordinate</code> to &lt;Point Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  coordinate      the <code>Coordinate</code> to process
   *@param  writer          the output writer to append to
   *@param  precisionModel  the <code>PrecisionModel</code> to use to convert
   *      from a precise coordinate to an external coordinate
   */
  private void appendPointText(Coordinate coordinate, int level, Writer writer,
      PrecisionModel precisionModel)
    throws IOException
  {
    if (coordinate == null) {
      writer.write("EMPTY");
    }
    else {
      writer.write("(");
      appendCoordinate(coordinate, writer);
      writer.write(")");
    }
  }

  /**
   * Appends the i'th coordinate from the sequence to the writer
   *
   * @param  seq  the <code>CoordinateSequence</code> to process
   * @param i     the index of the coordinate to write
   * @param  writer the output writer to append to
   */
  private void appendCoordinate(CoordinateSequence seq, int i, Writer writer)
      throws IOException
  {
    writer.write(writeNumber(seq.getX(i)) + " " + writeNumber(seq.getY(i)));
    if (outputDimension >= 3 && seq.getDimension() >= 3) {
      double z = seq.getOrdinate(i, 3);
      if (! Double.isNaN(z)) {
        writer.write(" ");
        writer.write(writeNumber(z));
      }
    }
  }

  /**
   *  Converts a <code>Coordinate</code> to <code>&lt;Point&gt;</code> format,
   *  then appends it to the writer.
   *
   *@param  coordinate      the <code>Coordinate</code> to process
   *@param  writer          the output writer to append to
   */
  private void appendCoordinate(Coordinate coordinate, Writer writer)
    throws IOException
  {
    writer.write(writeNumber(coordinate.x) + " " + writeNumber(coordinate.y));
    if (outputDimension >= 3 && ! Double.isNaN(coordinate.z)) {
      writer.write(" ");
      writer.write(writeNumber(coordinate.z));
    }
  }

  /**
   *  Converts a <code>double</code> to a <code>String</code>, not in scientific
   *  notation.
   *
   *@param  d  the <code>double</code> to convert
   *@return    the <code>double</code> as a <code>String</code>, not in
   *      scientific notation
   */
  private String writeNumber(double d) {
    return formatter.format(d);
  }

  /**
   *  Converts a <code>LineString</code> to &lt;LineString Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  lineString  the <code>LineString</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendSequenceText(CoordinateSequence seq, int level, boolean doIndent, Writer writer)
    throws IOException
  {
    if (seq.size() == 0) {
      writer.write("EMPTY");
    }
    else {
      if (doIndent) indent(level, writer);
      writer.write("(");
      for (int i = 0; i < seq.size(); i++) {
        if (i > 0) {
          writer.write(", ");
          if (coordsPerLine > 0
              && i % coordsPerLine == 0) {
            indent(level + 1, writer);
          }
        }
        appendCoordinate(seq, i, writer);
      }
      writer.write(")");
    }
  }

  /**
   *  Converts a <code>LineString</code> to &lt;LineString Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  lineString  the <code>LineString</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendLineStringText(LineString lineString, int level, boolean doIndent, Writer writer)
    throws IOException
  {
    if (lineString.isEmpty()) {
      writer.write("EMPTY");
    }
    else {
      if (doIndent) indent(level, writer);
      writer.write("(");
      for (int i = 0; i < lineString.getNumPoints(); i++) {
        if (i > 0) {
          writer.write(", ");
          if (coordsPerLine > 0
              && i % coordsPerLine == 0) {
            indent(level + 1, writer);
          }
        }
        appendCoordinate(lineString.getCoordinateN(i), writer);
      }
      writer.write(")");
    }
  }

  /**
   *  Converts a <code>Polygon</code> to &lt;Polygon Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  polygon  the <code>Polygon</code> to process
   *@param  writer   the output writer to append to
   */
  private void appendPolygonText(Polygon polygon, int level, boolean indentFirst, Writer writer)
    throws IOException
  {
    if (polygon.isEmpty()) {
      writer.write("EMPTY");
    }
    else {
      if (indentFirst) indent(level, writer);
      writer.write("(");
      appendLineStringText(polygon.getExteriorRing(), level, false, writer);
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        writer.write(", ");
        appendLineStringText(polygon.getInteriorRingN(i), level + 1, true, writer);
      }
      writer.write(")");
    }
  }

  /**
   *  Converts a <code>MultiPoint</code> to &lt;MultiPoint Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  multiPoint  the <code>MultiPoint</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendMultiPointText(MultiPoint multiPoint, int level, Writer writer)
    throws IOException
  {
    if (multiPoint.isEmpty()) {
      writer.write("EMPTY");
    }
    else {
      writer.write("(");
      for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          indentCoords(i, level + 1, writer);
        }
        writer.write("(");
        appendCoordinate(((Point) multiPoint.getGeometryN(i)).getCoordinate(), writer);
        writer.write(")");
     }
      writer.write(")");
    }
  }

  /**
   *  Converts a <code>MultiLineString</code> to &lt;MultiLineString Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multiLineString  the <code>MultiLineString</code> to process
   *@param  writer           the output writer to append to
   */
  private void appendMultiLineStringText(MultiLineString multiLineString, int level, boolean indentFirst,
      Writer writer)
    throws IOException
  {
    if (multiLineString.isEmpty()) {
      writer.write("EMPTY");
    }
    else {
      int level2 = level;
      boolean doIndent = indentFirst;
      writer.write("(");
      for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
          doIndent = true;
        }
        appendLineStringText((LineString) multiLineString.getGeometryN(i), level2, doIndent, writer);
      }
      writer.write(")");
    }
  }

  /**
   *  Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  multiPolygon  the <code>MultiPolygon</code> to process
   *@param  writer        the output writer to append to
   */
  private void appendMultiPolygonText(MultiPolygon multiPolygon, int level, Writer writer)
    throws IOException
  {
    if (multiPolygon.isEmpty()) {
      writer.write("EMPTY");
    }
    else {
      int level2 = level;
      boolean doIndent = false;
      writer.write("(");
      for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
          doIndent = true;
        }
        appendPolygonText((Polygon) multiPolygon.getGeometryN(i), level2, doIndent, writer);
      }
      writer.write(")");
    }
  }

  /**
   *  Converts a <code>GeometryCollection</code> to &lt;GeometryCollectionText&gt;
   *  format, then appends it to the writer.
   *
   *@param  geometryCollection  the <code>GeometryCollection</code> to process
   *@param  writer              the output writer to append to
   */
  private void appendGeometryCollectionText(GeometryCollection geometryCollection, int level,
      Writer writer)
    throws IOException
  {
    if (geometryCollection.isEmpty()) {
      writer.write("EMPTY");
    }
    else {
      int level2 = level;
      writer.write("(");
      for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
        }
        appendGeometryTaggedText(geometryCollection.getGeometryN(i), level2, writer);
      }
      writer.write(")");
    }
  }

  private void indentCoords(int coordIndex,  int level, Writer writer)
    throws IOException
  {
    if (coordsPerLine <= 0
        || coordIndex % coordsPerLine != 0)
      return;
    indent(level, writer);
  }

  private void indent(int level, Writer writer)
    throws IOException
  {
    if (! useFormatting || level <= 0)
      return;
    writer.write("\n");
    for (int i = 0; i < level; i++) {
      writer.write(indentTabStr);
    }
  }


}

