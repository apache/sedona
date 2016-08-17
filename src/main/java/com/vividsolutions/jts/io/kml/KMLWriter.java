package com.vividsolutions.jts.io.kml;

import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.util.StringUtil;

/**
 * Writes a formatted string containing the KML representation of a JTS
 * {@link Geometry}. 
 * The output is KML fragments which 
 * can be substituted wherever the KML <i>Geometry</i> abstract element can be used.
 * <p>
 * Output elements are indented to provide a
 * nicely-formatted representation. 
 * An output line prefix and maximum
 * number of coordinates per line can be specified.
 * <p>
 * The Z ordinate value output can be forced to be a specific value. 
 * The <code>extrude</code> and <code>altitudeMode</code> modes can be set. 
 * If set, the corresponding sub-elements will be output.
 */
public class KMLWriter 
{
  /**
   * The KML standard value <code>clampToGround</code> for use in {@link #setAltitudeMode(String)}.
   */
  public static String ALTITUDE_MODE_CLAMPTOGROUND = "clampToGround ";
  /**
   * The KML standard value <code>relativeToGround</code> for use in {@link #setAltitudeMode(String)}.
   */
  public static String ALTITUDE_MODE_RELATIVETOGROUND  = "relativeToGround  ";
  /**
   * The KML standard value <code>absolute</code> for use in {@link #setAltitudeMode(String)}.
   */
  public static String ALTITUDE_MODE_ABSOLUTE = "absolute";
  
  /**
   * Writes a Geometry as KML to a string, using
   * a specified Z value.
   * 
   * @param geometry the geometry to write
   * @param z the Z value to use
   * @return a string containing the KML geometry representation
   */
  public static String writeGeometry(Geometry geometry, double z) {
    KMLWriter writer = new KMLWriter();
    writer.setZ(z);
    return writer.write(geometry);
  }

  /**
    * Writes a Geometry as KML to a string, using
   * a specified Z value, precision, extrude flag,
   * and altitude mode code.
   * 
   * @param geometry the geometry to write
   * @param z the Z value to use
   * @param precision the maximum number of decimal places to write
   * @param extrude the extrude flag to write
   * @param altitudeMode the altitude model code to write
   * @return a string containing the KML geometry representation
   */
  public static String writeGeometry(Geometry geometry, double z, int precision,
      boolean extrude, String altitudeMode) {
    KMLWriter writer = new KMLWriter();
    writer.setZ(z);
    writer.setPrecision(precision);
    writer.setExtrude(extrude);
    writer.setAltitudeMode(altitudeMode);
    return writer.write(geometry);
  }

  private final int INDENT_SIZE = 2;
  private static final String COORDINATE_SEPARATOR = ",";
  private static final String TUPLE_SEPARATOR = " ";

  private String linePrefix = null;
  private int maxCoordinatesPerLine = 5;
  private double zVal = Double.NaN;
  private boolean extrude = false;
  private boolean tesselate;
  private String altitudeMode = null;
  private DecimalFormat numberFormatter = null;

  /**
   * Creates a new writer.
   */
  public KMLWriter() {
  }

  /**
   * Sets a tag string which is prefixed to every emitted text line.
   * This can be used to indent the geometry text in a containing document.
   * 
   * @param linePrefix the tag string
   */
  public void setLinePrefix(String linePrefix) {
    this.linePrefix = linePrefix;
  }

  /**
   * Sets the maximum number of coordinates to output per line.
   * 
   * @param maxCoordinatesPerLine the maximum number of coordinates to output
   */
  public void setMaximumCoordinatesPerLine(int maxCoordinatesPerLine) {
    if (maxCoordinatesPerLine <= 0) {
      maxCoordinatesPerLine = 1;
      return;
    }
    this.maxCoordinatesPerLine = maxCoordinatesPerLine;
  }

  /**
   * Sets the Z value to be output for all coordinates.
   * This overrides any Z value present in the Geometry coordinates.
   * 
   * @param zVal the Z value to output
   */
  public void setZ(double zVal) {
    this.zVal = zVal;
  }

  /**
   * Sets the flag to be output in the <code>extrude</code> element.
   * 
   * @param extrude the extrude flag to output
   */
  public void setExtrude(boolean extrude) {
    this.extrude = extrude;
  }

  /**
   * Sets the flag to be output in the <code>tesselate</code> element.
   * 
   * @param tesselate the tesselate flag to output
   */
  public void setTesselate(boolean tesselate) {
    this.tesselate = tesselate;
  }

  /**
   * Sets the value output in the <code>altitudeMode</code> element.
   * 
   * @param altitudeMode string representing the altitude mode
   */
  public void setAltitudeMode(String altitudeMode) {
    this.altitudeMode = altitudeMode;
  }

  /**
   * Sets the maximum nummber of decimal places to output in ordinate values.
   * Useful for limiting output size.
   * 
   * @param precision the number of decimal places to output
   */
  public void setPrecision(int precision) {
    //this.precision = precision;
    if (precision >= 0)
      numberFormatter = createFormatter(precision);
  }

  /**
   * Writes a {@link Geometry} in KML format as a string.
   * 
   * @param geom the geometry to write
   * @return a string containing the KML geometry representation
   */
  public String write(Geometry geom) {
    StringBuffer buf = new StringBuffer();
    write(geom, buf);
    return buf.toString();
  }

  /**
   * Writes the KML representation of a {@link Geometry} to a {@link Writer}.
   * 
   * @param geometry the geometry to write
   * @param writer the Writer to write to
   * @throws IOException if an I/O error occurred
   */
  public void write(Geometry geometry, Writer writer) throws IOException {
    writer.write(write(geometry));
  }

  /**
   * Appends the KML representation of a {@link Geometry} to a {@link StringBuffer}.
   * 
   * @param geometry the geometry to write
   * @param buf the buffer to write into
   */
  public void write(Geometry geometry, StringBuffer buf) {
    writeGeometry(geometry, 0, buf);
  }

  private void writeGeometry(Geometry g, int level, StringBuffer buf) {
    String attributes = "";
    if (g instanceof Point) {
      writePoint((Point) g, attributes, level, buf);
    } else if (g instanceof LinearRing) {
      writeLinearRing((LinearRing) g, attributes, true, level, buf);
    } else if (g instanceof LineString) {
      writeLineString((LineString) g, attributes, level, buf);
    } else if (g instanceof Polygon) {
      writePolygon((Polygon) g, attributes, level, buf);
    } else if (g instanceof GeometryCollection) {
      writeGeometryCollection((GeometryCollection) g, attributes, level, buf);
    }
    else 
      throw new IllegalArgumentException("Geometry type not supported: " + g.getGeometryType());
  }

  private void startLine(String text, int level, StringBuffer buf) {
    if (linePrefix != null)
      buf.append(linePrefix);
    buf.append(StringUtil.spaces(INDENT_SIZE * level));
    buf.append(text);
  }

  private String geometryTag(String geometryName, String attributes) {
    StringBuffer buf = new StringBuffer();
    buf.append("<");
    buf.append(geometryName);
    if (attributes != null && attributes.length() > 0) {
      buf.append(" ");
      buf.append(attributes);
    }
    buf.append(">");
    return buf.toString();
  }

  private void writeModifiers(int level, StringBuffer buf)
  {
    if (extrude) {
      startLine("<extrude>1</extrude>\n", level, buf);
    }
    if (tesselate) {
      startLine("<tesselate>1</tesselate>\n", level, buf);
    }
    if (altitudeMode != null) {
      startLine("<altitudeMode>" + altitudeMode + "</altitudeMode>\n", level, buf);
    }
  }
  
  private void writePoint(Point p, String attributes, int level,
      StringBuffer buf) {
  // <Point><coordinates>...</coordinates></Point>
    startLine(geometryTag("Point", attributes) + "\n", level, buf);
    writeModifiers(level, buf);
    write(new Coordinate[] { p.getCoordinate() }, level + 1, buf);
    startLine("</Point>\n", level, buf);
  }

  private void writeLineString(LineString ls, String attributes, int level,
      StringBuffer buf) {
  // <LineString><coordinates>...</coordinates></LineString>
    startLine(geometryTag("LineString", attributes) + "\n", level, buf);
    writeModifiers(level, buf);
    write(ls.getCoordinates(), level + 1, buf);
    startLine("</LineString>\n", level, buf);
  }

  private void writeLinearRing(LinearRing lr, String attributes, 
      boolean writeModifiers, int level,
      StringBuffer buf) {
  // <LinearRing><coordinates>...</coordinates></LinearRing>
    startLine(geometryTag("LinearRing", attributes) + "\n", level, buf);
    if (writeModifiers) writeModifiers(level, buf);
    write(lr.getCoordinates(), level + 1, buf);
    startLine("</LinearRing>\n", level, buf);
  }

  private void writePolygon(Polygon p, String attributes, int level,
      StringBuffer buf) {
    startLine(geometryTag("Polygon", attributes) + "\n", level, buf);
    writeModifiers(level, buf);

    startLine("  <outerBoundaryIs>\n", level, buf);
    writeLinearRing((LinearRing) p.getExteriorRing(), null, false, level + 1, buf);
    startLine("  </outerBoundaryIs>\n", level, buf);

    for (int t = 0; t < p.getNumInteriorRing(); t++) {
      startLine("  <innerBoundaryIs>\n", level, buf);
      writeLinearRing((LinearRing) p.getInteriorRingN(t), null, false, level + 1, buf);
      startLine("  </innerBoundaryIs>\n", level, buf);
    }

    startLine("</Polygon>\n", level, buf);
  }

  private void writeGeometryCollection(GeometryCollection gc,
      String attributes, int level, StringBuffer buf) {
    startLine("<MultiGeometry>\n", level, buf);
    for (int t = 0; t < gc.getNumGeometries(); t++) {
      writeGeometry(gc.getGeometryN(t), level + 1, buf);
    }
    startLine("</MultiGeometry>\n", level, buf);
  }

  /**
   * Takes a list of coordinates and converts it to KML.<br>
   * 2d and 3d aware. Terminates the coordinate output with a newline.
   * 
   * @param cs array of coordinates
   */
  private void write(Coordinate[] coords, int level, StringBuffer buf) {
    startLine("<coordinates>", level, buf);

    boolean isNewLine = false;
    for (int i = 0; i < coords.length; i++) {
      if (i > 0) {
        buf.append(TUPLE_SEPARATOR);
      }

      if (isNewLine) {
        startLine("  ", level, buf);
        isNewLine = false;
      }

      write(coords[i], buf);

      // break output lines to prevent them from getting too long
      if ((i + 1) % maxCoordinatesPerLine == 0 && i < coords.length - 1) {
        buf.append("\n");
        isNewLine = true;
      }
    }
    buf.append("</coordinates>\n");
  }

  private void write(Coordinate p, StringBuffer buf) {
    write(p.x, buf);
    buf.append(COORDINATE_SEPARATOR);
    write(p.y, buf);

    double z = p.z;
    // if altitude was specified directly, use it
    if (!Double.isNaN(zVal))
      z = zVal;

    // only write if Z present
    // MD - is this right? Or should it always be written?
    if (!Double.isNaN(z)) {
      buf.append(COORDINATE_SEPARATOR);
      write(z, buf);
    }
  }

  private void write(double num, StringBuffer buf) {
    if (numberFormatter != null)
      buf.append(numberFormatter.format(num));
    else
      buf.append(num);
  }

  /**
   * Creates the <code>DecimalFormat</code> used to write <code>double</code>s
   * with a sufficient number of decimal places.
   * 
   * @param precisionModel
   *          the <code>PrecisionModel</code> used to determine the number of
   *          decimal places to write.
   * @return a <code>DecimalFormat</code> that write <code>double</code> s
   *         without scientific notation.
   */
  private static DecimalFormat createFormatter(int precision) {
    // specify decimal separator explicitly to avoid problems in other locales
    DecimalFormatSymbols symbols = new DecimalFormatSymbols();
    symbols.setDecimalSeparator('.');
    DecimalFormat format = new DecimalFormat("0."
        + StringUtil.chars('#', precision), symbols);
    format.setDecimalSeparatorAlwaysShown(false);
    return format;
  }

}
