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
package org.apache.sedona.common.S2Geography;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.EnumSet;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.OrdinateFormat;
import org.locationtech.jts.io.WKTConstants;
import org.locationtech.jts.util.Assert;

public class WKTWriter {
  /**
   * Generates the WKT for a <tt>POINT</tt> specified by a {@link Coordinate}.
   *
   * @param p0 the point coordinate
   * @return the WKT
   */
  public static String toPoint(Coordinate p0) {
    return WKTConstants.POINT + " ( " + format(p0) + " )";
  }

  public static String format(Coordinate p) {
    return format(p.x, p.y);
  }

  private static String format(double x, double y) {
    return OrdinateFormat.DEFAULT.format(x) + " " + OrdinateFormat.DEFAULT.format(y);
  }

  private static final int INDENT = 2;
  private static final int OUTPUT_DIMENSION = 2;
  private boolean EWKT = false;

  /**
   * Creates the <code>DecimalFormat</code> used to write <code>double</code>s with a sufficient
   * number of decimal places.
   *
   * @param precisionModel the <code>PrecisionModel</code> used to determine the number of decimal
   *     places to write.
   * @return a <code>DecimalFormat</code> that write <code>double</code> s without scientific
   *     notation.
   */
  private static OrdinateFormat createFormatter(PrecisionModel precisionModel) {
    return OrdinateFormat.create(precisionModel.getMaximumSignificantDigits());
  }

  /**
   * Returns a <code>String</code> of repeated characters.
   *
   * @param ch the character to repeat
   * @param count the number of times to repeat the character
   * @return a <code>String</code> of characters
   */
  private static String stringOfChar(char ch, int count) {
    StringBuilder buf = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      buf.append(ch);
    }
    return buf.toString();
  }

  private EnumSet<Ordinate> outputOrdinates;
  private final int outputDimension;
  private PrecisionModel precisionModel = null;
  private OrdinateFormat ordinateFormat = null;
  private boolean isFormatted = false;
  private int coordsPerLine = -1;
  private String indentTabStr;

  /** Creates a new WKTWriter with default settings */
  public WKTWriter() {
    this(OUTPUT_DIMENSION);
  }

  public WKTWriter(boolean isEwkt) {
    this(OUTPUT_DIMENSION);
    this.EWKT = isEwkt;
  }

  /**
   * Creates a writer that writes {@link Geography}s with the given output dimension (2 to 4). The
   * output follows the following rules:
   *
   * <ul>
   *   <li>If the specified <b>output dimension is 3</b> and the <b>z is measure flag is set to
   *       true</b>, the Z value of coordinates will be written if it is present (i.e. if it is not
   *       <code>Double.NaN</code>)
   *   <li>If the specified <b>output dimension is 3</b> and the <b>z is measure flag is set to
   *       false</b>, the Measure value of coordinates will be written if it is present (i.e. if it
   *       is not <code>Double.NaN</code>)
   *   <li>If the specified <b>output dimension is 4</b>, the Z value of coordinates will be written
   *       even if it is not present when the Measure value is present. The Measure value of
   *       coordinates will be written if it is present (i.e. if it is not <code>Double.NaN</code>)
   * </ul>
   *
   * See also {@link #setOutputOrdinates(EnumSet)}
   *
   * @param outputDimension the coordinate dimension to output (2 to 4)
   */
  public WKTWriter(int outputDimension) {

    setTab(INDENT);
    this.outputDimension = outputDimension;
    setPrecisionModel(new PrecisionModel());

    if (outputDimension < 2 || outputDimension > 4)
      throw new IllegalArgumentException("Invalid output dimension (must be 2 to 4)");

    this.outputOrdinates = EnumSet.of(Ordinate.X, Ordinate.Y);
    if (outputDimension > 2) outputOrdinates.add(Ordinate.Z);
    if (outputDimension > 3) outputOrdinates.add(Ordinate.M);
  }

  /**
   * Sets whether the output will be formatted.
   *
   * @param isFormatted true if the output is to be formatted
   */
  public void setFormatted(boolean isFormatted) {
    this.isFormatted = isFormatted;
  }

  /**
   * Sets the maximum number of coordinates per line written in formatted output. If the provided
   * coordinate number is &lt;= 0, coordinates will be written all on one line.
   *
   * @param coordsPerLine the number of coordinates per line to output.
   */
  public void setMaxCoordinatesPerLine(int coordsPerLine) {
    this.coordsPerLine = coordsPerLine;
  }

  /**
   * Sets the tab size to use for indenting.
   *
   * @param size the number of spaces to use as the tab string
   * @throws IllegalArgumentException if the size is non-positive
   */
  public void setTab(int size) {
    if (size <= 0) throw new IllegalArgumentException("Tab count must be positive");
    this.indentTabStr = stringOfChar(' ', size);
  }

  /**
   * Sets the {@link Ordinate} that are to be written. Possible members are:
   *
   * <ul>
   *   <li>{@link Ordinate#X}
   *   <li>{@link Ordinate#Y}
   *   <li>{@link Ordinate#Z}
   *   <li>{@link Ordinate#M}
   * </ul>
   *
   * Values of {@link Ordinate#X} and {@link Ordinate#Y} are always assumed and not particularly
   * checked for.
   *
   * @param outputOrdinates A set of {@link Ordinate} values
   */
  public void setOutputOrdinates(EnumSet<Ordinate> outputOrdinates) {

    this.outputOrdinates.remove(Ordinate.Z);
    this.outputOrdinates.remove(Ordinate.M);

    if (this.outputDimension == 3) {
      if (outputOrdinates.contains(Ordinate.Z)) this.outputOrdinates.add(Ordinate.Z);
      else if (outputOrdinates.contains(Ordinate.M)) this.outputOrdinates.add(Ordinate.M);
    }
    if (this.outputDimension == 4) {
      if (outputOrdinates.contains(Ordinate.Z)) this.outputOrdinates.add(Ordinate.Z);
      if (outputOrdinates.contains(Ordinate.M)) this.outputOrdinates.add(Ordinate.M);
    }
  }

  /**
   * Gets a bit-pattern defining which ordinates should be
   *
   * @return an ordinate bit-pattern
   * @see #setOutputOrdinates(EnumSet)
   */
  public EnumSet<Ordinate> getOutputOrdinates() {
    return this.outputOrdinates;
  }

  /**
   * Sets a {@link PrecisionModel} that should be used on the ordinates written.
   *
   * <p>If none/{@code null} is assigned, the precision model of the {@link Geometry#getFactory()}
   * is used.
   *
   * <p>Note: The precision model is applied to all ordinate values, not just x and y.
   *
   * @param precisionModel the flag indicating if {@link Coordinate#z}/{} is actually a measure
   *     value.
   */
  public void setPrecisionModel(PrecisionModel precisionModel) {
    this.precisionModel = precisionModel;
    this.ordinateFormat = OrdinateFormat.create(precisionModel.getMaximumSignificantDigits());
  }

  /**
   * Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   * @param geography a <code>Geometry</code> to process
   * @return a &lt;Geometry Tagged Text&gt; string (see the OpenGIS Simple Features Specification)
   */
  public String write(Geography geography) {
    Writer sw = new StringWriter();

    try {
      writeFormatted(geography, false, sw);
    } catch (IOException ex) {
      Assert.shouldNeverReachHere();
    }
    return sw.toString();
  }

  /**
   * Converts a <code>geography</code> to its Well-known Text representation.
   *
   * @param geography a <code>geography</code> to process
   */
  public void write(Geography geography, Writer writer) throws IOException {
    // write the geometry
    writeFormatted(geography, isFormatted, writer);
  }

  /**
   * Converts a <code>geography</code> to its Well-known Text representation.
   *
   * @param geography a <code>geography</code> to process
   */
  private void writeFormatted(Geography geography, boolean useFormatting, Writer writer)
      throws IOException {
    OrdinateFormat formatter = getFormatter(geography);
    // append the WKT
    appendGeometryTaggedText(geography, useFormatting, writer, formatter);
  }

  private OrdinateFormat getFormatter(Geography geography) {
    // 1) If we’ve already created an OrdinateFormat, reuse it
    if (this.ordinateFormat != null) {
      return this.ordinateFormat;
    }

    PrecisionModel pm;
    if (this.precisionModel != null) {
      pm = this.precisionModel;
    } else {
      pm = new PrecisionModel();
    }
    this.ordinateFormat = createFormatter(pm);
    return this.ordinateFormat;
  }

  /**
   * Converts a <code>Geometry</code> to &lt;Geometry Tagged Text&gt; format, then appends it to the
   * writer.
   *
   * @param geography the <code>Geometry</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendGeometryTaggedText(
      Geography geography, boolean useFormatting, Writer writer, OrdinateFormat formatter)
      throws IOException {
    EnumSet<Ordinate> seq = getOutputOrdinates();
    // Append the WKT
    appendGeometryTaggedText(geography, seq, useFormatting, 0, writer, formatter);
  }
  /**
   * Converts a <code>Geometry</code> to &lt;Geometry Tagged Text&gt; format, then appends it to the
   * writer.
   *
   * @param geography the <code>Geometry</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendGeometryTaggedText(
      Geography geography,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    // indent before writing
    indent(useFormatting, level, writer);

    // ——— Handle Points ———
    if (geography instanceof SinglePointGeography) {
      appendPointTaggedText(
          (SinglePointGeography) geography,
          outputOrdinates,
          useFormatting,
          level,
          writer,
          formatter);
      return;
    }

    if (geography instanceof PointGeography) {
      appendMultiPointTaggedText(
          (PointGeography) geography, outputOrdinates, useFormatting, level, writer, formatter);
      return;
    }

    // ——— Handle LineStrings ———
    if (geography instanceof SinglePolylineGeography) {
      appendPolylineTaggedText(
          (SinglePolylineGeography) geography,
          outputOrdinates,
          useFormatting,
          level,
          writer,
          formatter);
      return;
    }

    if (geography instanceof PolylineGeography) {
      appendMultiLineStringTaggedText(
          (PolylineGeography) geography, outputOrdinates, useFormatting, level, writer, formatter);
      return;
    }

    // ——— Handle Polygons ———
    if (geography instanceof PolygonGeography) {
      appendPolygonTaggedText(
          (PolygonGeography) geography, outputOrdinates, useFormatting, level, writer, formatter);
      return;
    }

    if (geography instanceof MultiPolygonGeography) {
      appendMultiPolygonTaggedText(
          (MultiPolygonGeography) geography,
          outputOrdinates,
          useFormatting,
          level,
          writer,
          formatter);
      return;
    }

    // ——— Handle Collections ———
    if (geography instanceof GeographyCollection) {
      appendGeometryCollectionTaggedText(
          (GeographyCollection) geography,
          outputOrdinates,
          useFormatting,
          level,
          writer,
          formatter);
      return;
    }

    Assert.shouldNeverReachHere("Unsupported Geometry implementation: " + geography.getClass());
  }

  /** Check if need to write SRID */
  public void writeSRID(Geography geography, Writer writer) throws IOException {
    if (geography == null || writer == null) return;

    int srid = geography.getSRID();
    if (srid > 0) {
      writer.write("SRID=");
      writer.write(Integer.toString(srid));
      writer.write(';');
      writer.write(' ');
    }
  }
  /**
   * Converts a <code>Coordinate</code> to &lt;Point Tagged Text&gt; format, then appends it to the
   * writer.
   *
   * @param point the <code>Point</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the formatter to use when writing numbers
   */
  private void appendPointTaggedText(
      PointGeography point,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(point, writer);
    writer.write(WKTConstants.POINT);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendSequenceText(
        point.getCoordinateSequence(),
        outputOrdinates,
        useFormatting,
        level,
        false,
        writer,
        formatter);
  }

  /**
   * Converts a <code>LineString</code> to &lt;LineString Tagged Text&gt; format, then appends it to
   * the writer.
   *
   * @param lineString the <code>LineString</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendPolylineTaggedText(
      PolylineGeography lineString,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(lineString, writer);
    writer.write(WKTConstants.LINESTRING);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendSequenceText(
        lineString.getCoordinateSequence(),
        outputOrdinates,
        useFormatting,
        level,
        false,
        writer,
        formatter);
  }
  /**
   * Converts a <code>Polygon</code> to &lt;Polygon Tagged Text&gt; format, then appends it to the
   * writer.
   *
   * @param polygon the <code>Polygon</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendPolygonTaggedText(
      PolygonGeography polygon,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(polygon, writer);
    writer.write(WKTConstants.POLYGON);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendPolygonText(polygon, outputOrdinates, useFormatting, level, false, writer, formatter);
  }

  /**
   * Converts a <code>MultiPoint</code> to &lt;MultiPoint Tagged Text&gt; format, then appends it to
   * the writer.
   *
   * @param multipoint the <code>MultiPoint</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendMultiPointTaggedText(
      PointGeography multipoint,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(multipoint, writer);
    writer.write(WKTConstants.MULTIPOINT);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendMultiPointText(multipoint, outputOrdinates, useFormatting, level, writer, formatter);
  }

  /**
   * Converts a <code>MultiLineString</code> to &lt;MultiLineString Tagged Text&gt; format, then
   * appends it to the writer.
   *
   * @param multiLineString the <code>MultiLineString</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendMultiLineStringTaggedText(
      PolylineGeography multiLineString,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(multiLineString, writer);
    writer.write(WKTConstants.MULTILINESTRING);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendMultiLineStringText(
        multiLineString, outputOrdinates, useFormatting, level, /*false, */ writer, formatter);
  }

  /**
   * Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Tagged Text&gt; format, then appends
   * it to the writer.
   *
   * @param multiPolygon the <code>MultiPolygon</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendMultiPolygonTaggedText(
      MultiPolygonGeography multiPolygon,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(multiPolygon, writer);
    writer.write(WKTConstants.MULTIPOLYGON);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendMultiPolygonText(multiPolygon, outputOrdinates, useFormatting, level, writer, formatter);
  }

  /**
   * Converts a <code>GeometryCollection</code> to &lt;GeometryCollection Tagged Text&gt; format,
   * then appends it to the writer.
   *
   * @param geometryCollection the <code>GeometryCollection</code> to process
   * @param useFormatting flag indicating that the output should be formatted
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the <code>DecimalFormatter</code> to use to convert from a precise coordinate
   *     to an external coordinate
   */
  private void appendGeometryCollectionTaggedText(
      GeographyCollection geometryCollection,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (EWKT) writeSRID(geometryCollection, writer);
    writer.write(WKTConstants.GEOMETRYCOLLECTION);
    writer.write(" ");
    appendOrdinateText(outputOrdinates, writer);
    appendGeometryCollectionText(
        geometryCollection, outputOrdinates, useFormatting, level, writer, formatter);
  }

  /**
   * Appends the i'th coordinate from the sequence to the writer
   *
   * <p>If the {@code seq} has coordinates that are {@link double.NAN}, these are not written, even
   * though {@link #outputDimension} suggests this.
   *
   * @param seq the <code>CoordinateSequence</code> to process
   * @param i the index of the coordinate to write
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values
   */
  private void appendCoordinate(
      CoordinateSequence seq,
      EnumSet<Ordinate> outputOrdinates,
      int i,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    writer.write(writeNumber(seq.getX(i), formatter) + " " + writeNumber(seq.getY(i), formatter));

    if (outputOrdinates.contains(Ordinate.Z)) {
      writer.write(" ");
      writer.write(writeNumber(seq.getZ(i), formatter));
    }

    if (outputOrdinates.contains(Ordinate.M)) {
      writer.write(" ");
      writer.write(writeNumber(seq.getM(i), formatter));
    }
  }

  /**
   * Converts a <code>double</code> to a <code>String</code>, not in scientific notation.
   *
   * @param d the <code>double</code> to convert
   * @return the <code>double</code> as a <code>String</code>, not in scientific notation
   */
  private static String writeNumber(double d, OrdinateFormat formatter) {
    return formatter.format(d);
  }

  /**
   * Appends additional ordinate information. This function may
   *
   * <ul>
   *   <li>append 'Z' if in {@code outputOrdinates} the {@link Ordinate#Z} value is included
   *   <li>append 'M' if in {@code outputOrdinates} the {@link Ordinate#M} value is included
   *   <li>append 'ZM' if in {@code outputOrdinates} the {@link Ordinate#Z} and {@link Ordinate#M}
   *       values are included
   * </ul>
   *
   * @param outputOrdinates a bit-pattern of ordinates to write.
   * @param writer the output writer to append to.
   * @throws IOException if an error occurs while using the writer.
   */
  private void appendOrdinateText(EnumSet<Ordinate> outputOrdinates, Writer writer)
      throws IOException {

    if (outputOrdinates.contains(Ordinate.Z)) writer.append(WKTConstants.Z);
    if (outputOrdinates.contains(Ordinate.M)) writer.append(WKTConstants.M);
  }

  /**
   * Appends all members of a <code>CoordinateSequence</code> to the stream. Each {@code Coordinate}
   * is separated from another using a colon, the ordinates of a {@code Coordinate} are separated by
   * a space.
   *
   * @param seq the <code>CoordinateSequence</code> to process
   * @param useFormatting flag indicating that
   * @param level the indentation level
   * @param indentFirst flag indicating that the first {@code Coordinate} of the sequence should be
   *     indented for better visibility
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values.
   */
  private void appendSequenceText(
      CoordinateSequence seq,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      boolean indentFirst,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (seq.size() == 0) {
      writer.write(WKTConstants.EMPTY);
    } else {
      if (indentFirst) indent(useFormatting, level, writer);
      writer.write("(");
      for (int i = 0; i < seq.size(); i++) {
        if (i > 0) {
          writer.write(", ");
          if (coordsPerLine > 0 && i % coordsPerLine == 0) {
            indent(useFormatting, level + 1, writer);
          }
        }
        appendCoordinate(seq, outputOrdinates, i, writer, formatter);
      }
      writer.write(")");
    }
  }

  /**
   * Converts a <code>Polygon</code> to &lt;Polygon Text&gt; format, then appends it to the writer.
   *
   * @param polygon the <code>Polygon</code> to process
   * @param useFormatting flag indicating that
   * @param level the indentation level
   * @param indentFirst flag indicating that the first {@code Coordinate} of the sequence should be
   *     indented for better visibility
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values.
   */
  private void appendPolygonText(
      PolygonGeography polygon,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      boolean indentFirst,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (polygon == null || polygon.polygon.isEmpty()) {
      writer.write(WKTConstants.EMPTY);
    } else {
      if (indentFirst) indent(useFormatting, level, writer);
      writer.write("(");
      appendSequenceText(
          polygon.getExteriorRing().getCoordinateSequence(),
          outputOrdinates,
          useFormatting,
          level,
          false,
          writer,
          formatter);
      for (LineString hole : polygon.getLoops()) {
        writer.write(", ");
        appendSequenceText(
            hole.getCoordinateSequence(),
            outputOrdinates,
            useFormatting,
            level + 1,
            true,
            writer,
            formatter);
      }
      writer.write(")");
    }
  }

  /**
   * Converts a <code>MultiPoint</code> to &lt;MultiPoint Text&gt; format, then appends it to the
   * writer.
   *
   * @param multiPoint the <code>MultiPoint</code> to process
   * @param useFormatting flag indicating that
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values.
   */
  private void appendMultiPointText(
      PointGeography multiPoint,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (multiPoint.numShapes() == 0) {
      writer.write(WKTConstants.EMPTY);
    } else {
      writer.write("(");
      CoordinateSequence sequence = multiPoint.getCoordinateSequence();
      for (int i = 0; i < sequence.size(); i++) {
        if (i > 0) {
          writer.write(", ");
          indentCoords(useFormatting, i, level + 1, writer);
        }
        appendSequenceText(
            new SinglePointGeography(multiPoint.getPoints().get(i)).getCoordinateSequence(),
            outputOrdinates,
            useFormatting,
            level,
            false,
            writer,
            formatter);
      }
      writer.write(")");
    }
  }

  /**
   * Converts a <code>MultiLineString</code> to &lt;MultiLineString Text&gt; format, then appends it
   * to the writer.
   *
   * @param multiLineString the <code>MultiLineString</code> to process
   * @param useFormatting flag indicating that
   * @param level the indentation level //@param indentFirst flag indicating that the first {@code
   *     Coordinate} of the sequence should be indented for // better visibility
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values.
   */
  private void appendMultiLineStringText(
      PolylineGeography multiLineString,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level, /*boolean indentFirst, */
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (multiLineString.numShapes() == 0) {
      writer.write(WKTConstants.EMPTY);
    } else {
      int level2 = level;
      boolean doIndent = false;
      writer.write("(");
      for (int i = 0; i < multiLineString.numShapes(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
          doIndent = true;
        }
        appendSequenceText(
            new SinglePolylineGeography(multiLineString.getPolylines().get(i))
                .getCoordinateSequence(),
            outputOrdinates,
            useFormatting,
            level2,
            doIndent,
            writer,
            formatter);
      }
      writer.write(")");
    }
  }

  /**
   * Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Text&gt; format, then appends it to
   * the writer.
   *
   * @param multiPolygon the <code>MultiPolygon</code> to process
   * @param useFormatting flag indicating that
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values.
   */
  private void appendMultiPolygonText(
      MultiPolygonGeography multiPolygon,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (multiPolygon.numShapes() == 0) {
      writer.write(WKTConstants.EMPTY);
    } else {
      int level2 = level;
      boolean doIndent = false;
      writer.write("(");
      for (int i = 0; i < multiPolygon.numShapes(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
          doIndent = true;
        }
        appendPolygonText(
            (PolygonGeography) multiPolygon.getFeatures().get(i),
            outputOrdinates,
            useFormatting,
            level2,
            doIndent,
            writer,
            formatter);
      }
      writer.write(")");
    }
  }

  /**
   * Converts a <code>GeometryCollection</code> to &lt;GeometryCollectionText&gt; format, then
   * appends it to the writer.
   *
   * @param geometryCollection the <code>GeometryCollection</code> to process
   * @param useFormatting flag indicating that
   * @param level the indentation level
   * @param writer the output writer to append to
   * @param formatter the formatter to use for writing ordinate values.
   */
  private void appendGeometryCollectionText(
      GeographyCollection geometryCollection,
      EnumSet<Ordinate> outputOrdinates,
      boolean useFormatting,
      int level,
      Writer writer,
      OrdinateFormat formatter)
      throws IOException {
    if (geometryCollection.numShapes() == 0) {
      writer.write(WKTConstants.EMPTY);
    } else {
      int level2 = level;
      writer.write("(");
      for (int i = 0; i < geometryCollection.numShapes(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
        }
        appendGeometryTaggedText(
            geometryCollection.getFeatures().get(i),
            outputOrdinates,
            useFormatting,
            level2,
            writer,
            formatter);
      }
      writer.write(")");
    }
  }

  private void indentCoords(boolean useFormatting, int coordIndex, int level, Writer writer)
      throws IOException {
    if (coordsPerLine <= 0 || coordIndex % coordsPerLine != 0) return;
    indent(useFormatting, level, writer);
  }

  private void indent(boolean useFormatting, int level, Writer writer) throws IOException {
    if (!useFormatting || level <= 0) return;
    writer.write("\n");
    for (int i = 0; i < level; i++) {
      writer.write(indentTabStr);
    }
  }
}
