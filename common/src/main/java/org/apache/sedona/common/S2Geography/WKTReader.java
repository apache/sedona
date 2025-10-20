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

import com.google.common.geometry.*;
import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTConstants;
import org.locationtech.jts.util.Assert;

public class WKTReader {
  private static final String COMMA = ",";
  private static final String L_PAREN = "(";
  private static final String R_PAREN = ")";
  private static final String NAN_SYMBOL = "NaN";

  private GeometryFactory geometryFactory;
  private CoordinateSequenceFactory csFactory;
  private static CoordinateSequenceFactory csFactoryXYZM =
      CoordinateArraySequenceFactory.instance();
  private PrecisionModel precisionModel;

  /** Flag indicating that the old notation of coordinates in JTS is supported. */
  private static final boolean ALLOW_OLD_JTS_COORDINATE_SYNTAX = true;

  private boolean isAllowOldJtsCoordinateSyntax = ALLOW_OLD_JTS_COORDINATE_SYNTAX;

  /** Flag indicating that the old notation of MultiPoint coordinates in JTS is supported. */
  private static final boolean ALLOW_OLD_JTS_MULTIPOINT_SYNTAX = true;

  private boolean isAllowOldJtsMultipointSyntax = ALLOW_OLD_JTS_MULTIPOINT_SYNTAX;

  private boolean isFixStructure = false;

  public WKTReader() {
    this.geometryFactory = new GeometryFactory();
    this.csFactory = geometryFactory.getCoordinateSequenceFactory();
    this.precisionModel = geometryFactory.getPrecisionModel();
  }

  public WKTReader(GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
    this.csFactory = geometryFactory.getCoordinateSequenceFactory();
    this.precisionModel = geometryFactory.getPrecisionModel();
  }

  /**
   * Sets a flag indicating, that coordinates may have 3 ordinate values even though no Z or M
   * ordinate indicator is present. The default value is {@link #ALLOW_OLD_JTS_COORDINATE_SYNTAX}.
   *
   * @param value a boolean value
   */
  public void setIsOldJtsCoordinateSyntaxAllowed(boolean value) {
    isAllowOldJtsCoordinateSyntax = value;
  }

  /**
   * Sets a flag indicating, that point coordinates in a MultiPoint geometry must not be enclosed in
   * paren. The default value is {@link #ALLOW_OLD_JTS_MULTIPOINT_SYNTAX}
   *
   * @param value a boolean value
   */
  public void setIsOldJtsMultiPointSyntaxAllowed(boolean value) {
    isAllowOldJtsMultipointSyntax = value;
  }

  /**
   * Sets a flag indicating that the structure of input geometry should be fixed so that the
   * geometry can be constructed without error. This involves adding coordinates if the input
   * coordinate sequence is shorter than required.
   *
   * @param isFixStructure true if the input structure should be fixed
   * @see LinearRing#MINIMUM_VALID_SIZE
   */
  public void setFixStructure(boolean isFixStructure) {
    this.isFixStructure = isFixStructure;
  }

  /**
   * Reads a Well-Known Text representation of a {@link Geometry} from a {@link String}.
   *
   * @param wellKnownText one or more &lt;Geometry Tagged Text&gt; strings (see the OpenGIS Simple
   *     Features Specification) separated by whitespace
   * @return a <code>Geometry</code> specified by <code>wellKnownText</code>
   * @throws ParseException if a parsing problem occurs
   */
  public Geography read(String wellKnownText) throws ParseException {
    StringReader reader = new StringReader(wellKnownText);
    try {
      return read(reader);
    } finally {
      reader.close();
    }
  }

  /**
   * Reads a Well-Known Text representation of a {@link
   * org.apache.sedona.common.S2Geography.Geography} from a {@link Reader}.
   *
   * @param reader a Reader which will return a &lt;Geometry Tagged Text&gt; string (see the OpenGIS
   *     Simple Features Specification)
   * @return a <code>Geometry</code> read from <code>reader</code>
   * @throws ParseException if a parsing problem occurs
   */
  public Geography read(Reader reader) throws ParseException {
    StreamTokenizer tokenizer = createTokenizer(reader);
    try {
      return readGeometryTaggedText(tokenizer);
    } catch (IOException e) {
      throw new ParseException(e.toString());
    }
  }

  /**
   * Utility function to create the tokenizer
   *
   * @param reader a reader
   * @return a WKT Tokenizer.
   */
  private static StreamTokenizer createTokenizer(Reader reader) {
    StreamTokenizer tokenizer = new StreamTokenizer(reader);
    // set tokenizer to NOT parse numbers
    tokenizer.resetSyntax();
    tokenizer.wordChars('a', 'z');
    tokenizer.wordChars('A', 'Z');
    tokenizer.wordChars(128 + 32, 255);
    tokenizer.wordChars('0', '9');
    tokenizer.wordChars('-', '-');
    tokenizer.wordChars('+', '+');
    tokenizer.wordChars('.', '.');
    tokenizer.whitespaceChars(0, ' ');
    tokenizer.commentChar('#');

    return tokenizer;
  }

  /**
   * Reads a <code>Coordinate</Code> from a stream using the given {@link StreamTokenizer}.
   *
   * <p>All ordinate values are read, but -depending on the {@link CoordinateSequenceFactory} of the
   * underlying {@link GeometryFactory}- not necessarily all can be handled. Those are silently
   * dropped.
   *
   * @param tokenizer the tokenizer to use
   * @param ordinateFlags a bit-mask defining the ordinates to read.
   * @param tryParen a value indicating if a starting {@link #L_PAREN} should be probed.
   * @return a {@link Coordinate} of appropriate dimension containing the read ordinate values
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private Coordinate getCoordinate(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags, boolean tryParen)
      throws IOException, ParseException {
    boolean opened = false;
    if (tryParen && isOpenerNext(tokenizer)) {
      tokenizer.nextToken();
      opened = true;
    }

    // create a sequence for one coordinate
    int offsetM = ordinateFlags.contains(Ordinate.Z) ? 1 : 0;
    Coordinate coord = createCoordinate(ordinateFlags);
    coord.setOrdinate(CoordinateSequence.X, precisionModel.makePrecise(getNextNumber(tokenizer)));
    coord.setOrdinate(CoordinateSequence.Y, precisionModel.makePrecise(getNextNumber(tokenizer)));

    // additionally read other vertices
    if (ordinateFlags.contains(Ordinate.Z))
      coord.setOrdinate(CoordinateSequence.Z, getNextNumber(tokenizer));
    if (ordinateFlags.contains(Ordinate.M))
      coord.setOrdinate(CoordinateSequence.Z + offsetM, getNextNumber(tokenizer));

    if (ordinateFlags.size() == 2
        && this.isAllowOldJtsCoordinateSyntax
        && isNumberNext(tokenizer)) {
      coord.setOrdinate(CoordinateSequence.Z, getNextNumber(tokenizer));
    }

    // read close token if it was opened here
    if (opened) {
      getNextCloser(tokenizer);
    }

    return coord;
  }

  private Coordinate createCoordinate(EnumSet<Ordinate> ordinateFlags) {
    boolean hasZ = ordinateFlags.contains(Ordinate.Z);
    boolean hasM = ordinateFlags.contains(Ordinate.M);
    if (hasZ && hasM) return new CoordinateXYZM();
    if (hasM) return new CoordinateXYM();
    if (hasZ || this.isAllowOldJtsCoordinateSyntax) return new Coordinate();
    return new CoordinateXY();
  }

  /**
   * Reads a <code>Coordinate</Code> from a stream using the given {@link StreamTokenizer}.
   *
   * <p>All ordinate values are read, but -depending on the {@link CoordinateSequenceFactory} of the
   * underlying {@link GeometryFactory}- not necessarily all can be handled. Those are silently
   * dropped.
   *
   * <p>
   *
   * @param tokenizer the tokenizer to use
   * @param ordinateFlags a bit-mask defining the ordinates to read.
   * @return a {@link CoordinateSequence} of length 1 containing the read ordinate values
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private CoordinateSequence getCoordinateSequence(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags, int minSize, boolean isRing)
      throws IOException, ParseException {
    if (getNextEmptyOrOpener(tokenizer).equals(WKTConstants.EMPTY))
      return createCoordinateSequenceEmpty(ordinateFlags);

    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    do {
      coordinates.add(getCoordinate(tokenizer, ordinateFlags, false));
    } while (getNextCloserOrComma(tokenizer).equals(COMMA));

    if (isFixStructure) {
      fixStructure(coordinates, minSize, isRing);
    }
    Coordinate[] coordArray = coordinates.toArray(new Coordinate[0]);
    return csFactory.create(coordArray);
  }

  private static void fixStructure(List<Coordinate> coords, int minSize, boolean isRing) {
    if (coords.size() == 0) return;
    if (isRing && !isClosed(coords)) {
      coords.add(coords.get(0).copy());
    }
    while (coords.size() < minSize) {
      coords.add(coords.get(coords.size() - 1).copy());
    }
  }

  private static boolean isClosed(List<Coordinate> coords) {
    if (coords.size() == 0) return true;
    if (coords.size() == 1 || !coords.get(0).equals2D(coords.get(coords.size() - 1))) {
      return false;
    }
    return true;
  }

  private CoordinateSequence createCoordinateSequenceEmpty(EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    return csFactory.create(
        0, toDimension(ordinateFlags), ordinateFlags.contains(Ordinate.M) ? 1 : 0);
  }

  /**
   * Reads a <code>CoordinateSequence</Code> from a stream using the given {@link StreamTokenizer}
   * for an old-style JTS MultiPoint (Point coordinates not enclosed in parentheses).
   *
   * <p>All ordinate values are read, but -depending on the {@link CoordinateSequenceFactory} of the
   * underlying {@link GeometryFactory}- not necessarily all can be handled. Those are silently
   * dropped.
   *
   * @param tokenizer the tokenizer to use
   * @param ordinateFlags a bit-mask defining the ordinates to read.
   * @return a {@link CoordinateSequence} of length 1 containing the read ordinate values
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered S
   */
  private CoordinateSequence getCoordinateSequenceOldMultiPoint(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {

    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    do {
      coordinates.add(getCoordinate(tokenizer, ordinateFlags, true));
    } while (getNextCloserOrComma(tokenizer).equals(COMMA));

    Coordinate[] coordArray = coordinates.toArray(new Coordinate[0]);
    return csFactory.create(coordArray);
  }

  /**
   * Computes the required dimension based on the given ordinate values. It is assumed that {@link
   * Ordinate#X} and {@link Ordinate#Y} are included.
   *
   * @param ordinateFlags the ordinate bit-mask
   * @return the number of dimensions required to store ordinates for the given bit-mask.
   */
  private int toDimension(EnumSet<Ordinate> ordinateFlags) {
    int dimension = 2;
    if (ordinateFlags.contains(Ordinate.Z)) dimension++;
    if (ordinateFlags.contains(Ordinate.M)) dimension++;

    if (dimension == 2 && this.isAllowOldJtsCoordinateSyntax) dimension++;

    return dimension;
  }

  /**
   * Tests if the next token in the stream is a number
   *
   * @param tokenizer the tokenizer
   * @return {@code true} if the next token is a number, otherwise {@code false}
   * @throws IOException if an I/O error occurs
   */
  private static boolean isNumberNext(StreamTokenizer tokenizer) throws IOException {
    int type = tokenizer.nextToken();
    tokenizer.pushBack();
    return type == StreamTokenizer.TT_WORD;
  }

  /**
   * Tests if the next token in the stream is a left opener ({@link #L_PAREN})
   *
   * @param tokenizer the tokenizer
   * @return {@code true} if the next token is a {@link #L_PAREN}, otherwise {@code false}
   * @throws IOException if an I/O error occurs
   */
  private static boolean isOpenerNext(StreamTokenizer tokenizer) throws IOException {
    int type = tokenizer.nextToken();
    tokenizer.pushBack();
    return type == '(';
  }

  /**
   * Parses the next number in the stream. Numbers with exponents are handled. <tt>NaN</tt> values
   * are handled correctly, and the case of the "NaN" symbol is not significant.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text
   * @return the next number in the stream
   * @throws ParseException if the next token is not a valid number
   * @throws IOException if an I/O error occurs
   */
  private double getNextNumber(StreamTokenizer tokenizer) throws IOException, ParseException {
    int type = tokenizer.nextToken();
    switch (type) {
      case StreamTokenizer.TT_WORD:
        {
          if (tokenizer.sval.equalsIgnoreCase(NAN_SYMBOL)) {
            return Double.NaN;
          } else {
            try {
              return Double.parseDouble(tokenizer.sval);
            } catch (NumberFormatException ex) {
              throw parseErrorWithLine(tokenizer, "Invalid number: " + tokenizer.sval);
            }
          }
        }
    }
    throw parseErrorExpected(tokenizer, "number");
  }

  /**
   * Returns the next EMPTY or L_PAREN in the stream as uppercase text.
   *
   * @return the next EMPTY or L_PAREN in the stream as uppercase text.
   * @throws ParseException if the next token is not EMPTY or L_PAREN
   * @throws IOException if an I/O error occurs
   * @param tokenizer tokenizer over a stream of text in Well-known Text
   */
  private static String getNextEmptyOrOpener(StreamTokenizer tokenizer)
      throws IOException, ParseException {
    String nextWord = getNextWord(tokenizer);
    if (nextWord.equalsIgnoreCase(WKTConstants.Z)) {
      // z = true;
      nextWord = getNextWord(tokenizer);
    } else if (nextWord.equalsIgnoreCase(WKTConstants.M)) {
      // m = true;
      nextWord = getNextWord(tokenizer);
    } else if (nextWord.equalsIgnoreCase(WKTConstants.ZM)) {
      // z = true;
      // m = true;
      nextWord = getNextWord(tokenizer);
    }
    if (nextWord.equals(WKTConstants.EMPTY) || nextWord.equals(L_PAREN)) {
      return nextWord;
    }
    throw parseErrorExpected(tokenizer, WKTConstants.EMPTY + " or " + L_PAREN);
  }

  /**
   * Returns the next ordinate flag information in the stream as uppercase text. This can be Z, M or
   * ZM.
   *
   * @return the next EMPTY or L_PAREN in the stream as uppercase text.
   * @throws ParseException if the next token is not EMPTY or L_PAREN
   * @throws IOException if an I/O error occurs
   * @param tokenizer tokenizer over a stream of text in Well-known Text
   */
  private static EnumSet<Ordinate> getNextOrdinateFlags(StreamTokenizer tokenizer)
      throws IOException, ParseException {

    EnumSet<Ordinate> result = EnumSet.of(Ordinate.X, Ordinate.Y);

    String nextWord = lookAheadWord(tokenizer).toUpperCase(Locale.ROOT);
    if (nextWord.equalsIgnoreCase(WKTConstants.Z)) {
      tokenizer.nextToken();
      result.add(Ordinate.Z);
    } else if (nextWord.equalsIgnoreCase(WKTConstants.M)) {
      tokenizer.nextToken();
      result.add(Ordinate.M);
    } else if (nextWord.equalsIgnoreCase(WKTConstants.ZM)) {
      tokenizer.nextToken();
      result.add(Ordinate.Z);
      result.add(Ordinate.M);
    }
    return result;
  }

  /**
   * Returns the next word in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next token must
   *     be a word.
   * @return the next word in the stream as uppercase text
   * @throws ParseException if the next token is not a word
   * @throws IOException if an I/O error occurs
   */
  private static String lookAheadWord(StreamTokenizer tokenizer)
      throws IOException, ParseException {
    String nextWord = getNextWord(tokenizer);
    tokenizer.pushBack();
    return nextWord;
  }

  /**
   * Returns the next {@link #R_PAREN} or {@link #COMMA} in the stream.
   *
   * @return the next R_PAREN or COMMA in the stream
   * @throws ParseException if the next token is not R_PAREN or COMMA
   * @throws IOException if an I/O error occurs
   * @param tokenizer tokenizer over a stream of text in Well-known Text
   */
  private static String getNextCloserOrComma(StreamTokenizer tokenizer)
      throws IOException, ParseException {
    String nextWord = getNextWord(tokenizer);
    if (nextWord.equals(COMMA) || nextWord.equals(R_PAREN)) {
      return nextWord;
    }
    throw parseErrorExpected(tokenizer, COMMA + " or " + R_PAREN);
  }

  /**
   * Returns the next {@link #R_PAREN} in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next token must
   *     be R_PAREN.
   * @return the next R_PAREN in the stream
   * @throws ParseException if the next token is not R_PAREN
   * @throws IOException if an I/O error occurs
   */
  private String getNextCloser(StreamTokenizer tokenizer) throws IOException, ParseException {
    String nextWord = getNextWord(tokenizer);
    if (nextWord.equals(R_PAREN)) {
      return nextWord;
    }
    throw parseErrorExpected(tokenizer, R_PAREN);
  }

  /**
   * Returns the next word in the stream.
   *
   * @return the next word in the stream as uppercase text
   * @throws ParseException if the next token is not a word
   * @throws IOException if an I/O error occurs
   * @param tokenizer tokenizer over a stream of text in Well-known Text
   */
  private static String getNextWord(StreamTokenizer tokenizer) throws IOException, ParseException {
    int type = tokenizer.nextToken();
    switch (type) {
      case StreamTokenizer.TT_WORD:
        String word = tokenizer.sval;
        if (word.equalsIgnoreCase(WKTConstants.EMPTY)) return WKTConstants.EMPTY;
        return word;

      case '(':
        return L_PAREN;
      case ')':
        return R_PAREN;
      case ',':
        return COMMA;
    }
    throw parseErrorExpected(tokenizer, "word");
  }

  /**
   * Creates a formatted ParseException reporting that the current token was unexpected.
   *
   * @param expected a description of what was expected
   */
  private static ParseException parseErrorExpected(StreamTokenizer tokenizer, String expected) {
    // throws Asserts for tokens that should never be seen
    if (tokenizer.ttype == StreamTokenizer.TT_NUMBER)
      Assert.shouldNeverReachHere("Unexpected NUMBER token");
    if (tokenizer.ttype == StreamTokenizer.TT_EOL)
      Assert.shouldNeverReachHere("Unexpected EOL token");

    String tokenStr = tokenString(tokenizer);
    return parseErrorWithLine(tokenizer, "Expected " + expected + " but found " + tokenStr);
  }

  /**
   * Creates a formatted ParseException reporting that the current token was unexpected.
   *
   * @param msg a description of what was expected
   */
  private static ParseException parseErrorWithLine(StreamTokenizer tokenizer, String msg) {
    return new ParseException(msg + " (line " + tokenizer.lineno() + ")");
  }

  /**
   * Gets a description of the current token type
   *
   * @param tokenizer the tokenizer
   * @return a description of the current token
   */
  private static String tokenString(StreamTokenizer tokenizer) {
    switch (tokenizer.ttype) {
      case StreamTokenizer.TT_NUMBER:
        return "<NUMBER>";
      case StreamTokenizer.TT_EOL:
        return "End-of-Line";
      case StreamTokenizer.TT_EOF:
        return "End-of-Stream";
      case StreamTokenizer.TT_WORD:
        return "'" + tokenizer.sval + "'";
    }
    return "'" + (char) tokenizer.ttype + "'";
  }

  /**
   * Creates a <code>Geometry</code> using the next token in the stream.
   *
   * @return a <code>Geometry</code> specified by the next token in the stream
   * @throws ParseException if the coordinates used to create a <code>Polygon</code> shell and holes
   *     do not form closed linestrings, or if an unexpected token was encountered
   * @throws IOException if an I/O error occurs
   * @param tokenizer tokenizer over a stream of text in Well-known Text
   */
  private org.apache.sedona.common.S2Geography.Geography readGeometryTaggedText(
      StreamTokenizer tokenizer) throws IOException, ParseException {
    String type;

    EnumSet<Ordinate> ordinateFlags = EnumSet.of(Ordinate.X, Ordinate.Y);
    type = getNextWord(tokenizer).toUpperCase(Locale.ROOT);
    if (type.endsWith(WKTConstants.ZM)) {
      ordinateFlags.add(Ordinate.Z);
      ordinateFlags.add(Ordinate.M);
    } else if (type.endsWith(WKTConstants.Z)) {
      ordinateFlags.add(Ordinate.Z);
    } else if (type.endsWith(WKTConstants.M)) {
      ordinateFlags.add(Ordinate.M);
    }
    return readGeometryTaggedText(tokenizer, type, ordinateFlags);
  }

  private org.apache.sedona.common.S2Geography.Geography readGeometryTaggedText(
      StreamTokenizer tokenizer, String type, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {

    if (ordinateFlags.size() == 2) {
      ordinateFlags = getNextOrdinateFlags(tokenizer);
    }

    // if we can create a sequence with the required dimension everything is ok, otherwise
    // we need to take a different coordinate sequence factory.
    // It would be good to not have to try/catch this but if the CoordinateSequenceFactory
    // exposed a value indicating which min/max dimension it can handle or even an
    // ordinate bit-flag.
    try {
      csFactory.create(0, toDimension(ordinateFlags), ordinateFlags.contains(Ordinate.M) ? 1 : 0);
    } catch (Exception e) {
      geometryFactory =
          new GeometryFactory(
              geometryFactory.getPrecisionModel(), geometryFactory.getSRID(), csFactoryXYZM);
    }

    if (isTypeName(tokenizer, type, WKTConstants.POINT)) {
      return readPointText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.LINESTRING)) {
      return readPolylineText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.LINEARRING)) {
      return readPolygonText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.POLYGON)) {
      return readPolygonText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.MULTIPOINT)) {
      return readMultiPointText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.MULTILINESTRING)) {
      return readMultiPolylineText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.MULTIPOLYGON)) {
      return readMultiPolygonText(tokenizer, ordinateFlags);
    } else if (isTypeName(tokenizer, type, WKTConstants.GEOMETRYCOLLECTION)) {
      return readGeographyCollectionText(tokenizer, ordinateFlags);
    }
    throw parseErrorWithLine(tokenizer, "Unknown geography type: " + type);
  }

  private boolean isTypeName(StreamTokenizer tokenizer, String type, String typeName)
      throws ParseException {
    if (!type.startsWith(typeName)) return false;

    String modifiers = type.substring(typeName.length());
    boolean isValidMod =
        modifiers.length() <= 2
            && (modifiers.length() == 0
                || modifiers.equals(WKTConstants.Z)
                || modifiers.equals(WKTConstants.M)
                || modifiers.equals(WKTConstants.ZM));
    if (!isValidMod) {
      throw parseErrorWithLine(tokenizer, "Invalid dimension modifiers: " + type);
    }

    return true;
  }

  /**
   * Creates a <code>Point</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;Point Text&gt;.
   * @return a <code>Point</code> specified by the next token in the stream
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private SinglePointGeography readPointText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    CoordinateSequence pts = getCoordinateSequence(tokenizer, ordinateFlags, 1, false);
    // If X and Y are NaN create a empty point
    if (pts.size() <= 0 || Double.isNaN(pts.getX(0)) || Double.isNaN(pts.getY(0))) {
      return new SinglePointGeography();
    }
    double lon = pts.getX(0);
    double lat = pts.getY(0);
    S2Point s2Point = S2LatLng.fromDegrees(lat, lon).toPoint();

    // Build via S2Builder + S2PointVectorLayer
    S2Builder builder = new S2Builder.Builder().build();
    S2PointVectorLayer layer = new S2PointVectorLayer();
    builder.startLayer(layer);
    builder.addPoint(s2Point);

    // must call build() before reading out the points
    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("Failed to build S2 point layer: " + error.text());
    }
    // Extract the resulting points
    List<S2Point> points = layer.getPointVector();
    if (points.isEmpty()) {
      return new SinglePointGeography();
    }
    return new SinglePointGeography(points.get(0));
  }

  /**
   * Creates a <code>LineString</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;LineString Text&gt;.
   * @return a <code>LineString</code> specified by the next token in the stream
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private SinglePolylineGeography readPolylineText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    CoordinateSequence seq =
        getCoordinateSequence(tokenizer, ordinateFlags, LineString.MINIMUM_VALID_SIZE, false);
    if (seq.size() < 2) {
      // empty or extended-but-all-NaN → empty geography
      return new SinglePolylineGeography();
    }

    List<S2Point> pts = new ArrayList<>(seq.size());
    for (int i = 0; i < seq.size(); i++) {
      double lon = seq.getX(i);
      double lat = seq.getY(i);
      pts.add(S2LatLng.fromDegrees(lat, lon).toPoint());
    }

    S2Builder builder = new S2Builder.Builder().build();
    S2PolylineLayer layer = new S2PolylineLayer();
    builder.startLayer(layer);

    builder.addPolyline(new S2Polyline(pts));

    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("Failed to build S2 polyline: " + error.text());
    }
    S2Polyline s2poly = layer.getPolyline();
    return new SinglePolylineGeography(s2poly);
  }

  /**
   * Creates a <code>LinearRing</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;LineString Text&gt;.
   * @return a <code>LinearRing</code> specified by the next token in the stream
   * @throws IOException if an I/O error occurs
   * @throws ParseException if the coordinates used to create the <code>LinearRing</code> do not
   *     form a closed linestring, or if an unexpected token was encountered
   */
  private S2Loop readLoopText(StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    CoordinateSequence seq =
        getCoordinateSequence(tokenizer, ordinateFlags, LinearRing.MINIMUM_VALID_SIZE, true);
    // build the loop
    List<S2Point> pts = new ArrayList<>(seq.size());
    for (int i = 0; i < seq.size(); i++) {
      pts.add(S2LatLng.fromDegrees(seq.getY(i), seq.getX(i)).toPoint());
    }
    return new S2Loop(pts);
  }

  /**
   * Creates a <code>MultiPoint</code> using the next tokens in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;MultiPoint Text&gt;.
   * @return a <code>MultiPoint</code> specified by the next token in the stream
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private PointGeography readMultiPointText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener(tokenizer);
    if (nextToken.equals(WKTConstants.EMPTY)) {
      return new PointGeography();
    }

    // check for old-style JTS syntax (no parentheses surrounding Point coordinates) and parse it if
    // present
    // MD 2009-02-21 - this is only provided for backwards compatibility for a few versions
    if (isAllowOldJtsMultipointSyntax) {
      String nextWord = lookAheadWord(tokenizer);
      if (nextWord != L_PAREN && nextWord != WKTConstants.EMPTY) {
        CoordinateSequence pts = getCoordinateSequenceOldMultiPoint(tokenizer, ordinateFlags);
        if (Double.isNaN(pts.getX(0)) || Double.isNaN(pts.getY(0))) {
          return new PointGeography();
        }
        List<S2Point> points = new ArrayList<>(pts.size());
        // Build via S2Builder + S2PointVectorLayer
        S2Builder builder = new S2Builder.Builder().build();
        S2PointVectorLayer layer = new S2PointVectorLayer();
        // must call build() before reading out the points
        S2Error error = new S2Error();
        for (int i = 0; i < pts.size(); i++) {
          double lon = pts.getX(i);
          double lat = pts.getY(i);
          S2Point s2Point = S2LatLng.fromDegrees(lat, lon).toPoint();

          builder.startLayer(layer);
          builder.addPoint(s2Point);

          if (!builder.build(error)) {
            throw new IOException("Failed to build S2 point layer: " + error.text());
          }
        }
        for (int i = 0; i < layer.getPointVector().size(); i++) {
          // Extract the resulting points
          points.add(layer.getPointVector().get(i));
        }
        return new PointGeography(points);
      }
    }

    List<S2Point> points = new ArrayList<S2Point>();
    PointGeography point = readPointText(tokenizer, ordinateFlags);
    points.addAll(point.getPoints());
    nextToken = getNextCloserOrComma(tokenizer);
    while (nextToken.equals(COMMA)) {
      point = readPointText(tokenizer, ordinateFlags);
      points.addAll(point.getPoints());
      nextToken = getNextCloserOrComma(tokenizer);
    }
    return new PointGeography(points);
  }

  /**
   * Creates a <code>Polygon</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;Polygon Text&gt;.
   * @return a <code>Polygon</code> specified by the next token in the stream
   * @throws ParseException if the coordinates used to create the <code>Polygon</code> shell and
   *     holes do not form closed linestrings, or if an unexpected token was encountered.
   * @throws IOException if an I/O error occurs
   */
  private PolygonGeography readPolygonText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener(tokenizer);
    if (nextToken.equals(WKTConstants.EMPTY)) {
      return new PolygonGeography();
    }

    List<S2Loop> holes = new ArrayList<S2Loop>();

    S2Loop shell = readLoopText(tokenizer, ordinateFlags);
    holes.add(shell);
    nextToken = getNextCloserOrComma(tokenizer);
    while (nextToken.equals(COMMA)) {
      S2Loop hole = readLoopText(tokenizer, ordinateFlags);
      holes.add(hole);
      nextToken = getNextCloserOrComma(tokenizer);
    }

    // Now feed those loops into S2Builder + S2PolygonLayer:
    S2Builder builder = new S2Builder.Builder().build();
    S2PolygonLayer polyLayer = new S2PolygonLayer();
    builder.startLayer(polyLayer);

    // add shell + holes
    for (S2Loop loop : holes) {
      builder.addLoop(loop);
    }

    // build
    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("S2Builder failed: " + error.text());
    }

    // extract the stitched polygon
    S2Polygon s2poly = polyLayer.getPolygon();

    // wrap in your PolygonGeography
    return new PolygonGeography(s2poly);
  }

  /**
   * Creates a <code>MultiLineString</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;MultiLineString Text&gt;.
   * @return a <code>MultiLineString</code> specified by the next token in the stream
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private PolylineGeography readMultiPolylineText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener(tokenizer);
    if (nextToken.equals(WKTConstants.EMPTY)) {
      return new PolylineGeography();
    }

    List<S2Polyline> lineStrings = new ArrayList<S2Polyline>();
    do {
      PolylineGeography lineString = readPolylineText(tokenizer, ordinateFlags);
      lineStrings.addAll(lineString.getPolylines());
      nextToken = getNextCloserOrComma(tokenizer);
    } while (nextToken.equals(COMMA));
    return new PolylineGeography(lineStrings);
  }

  /**
   * Creates a <code>MultiPolygon</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;MultiPolygon Text&gt;.
   * @return a <code>MultiPolygon</code> specified by the next token in the stream, or if if the
   *     coordinates used to create the <code>Polygon</code> shells and holes do not form closed
   *     linestrings.
   * @throws IOException if an I/O error occurs
   * @throws ParseException if an unexpected token was encountered
   */
  private MultiPolygonGeography readMultiPolygonText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener(tokenizer);
    if (nextToken.equals(WKTConstants.EMPTY)) {
      return new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, new ArrayList<>());
    }
    List<S2Polygon> polygons = new ArrayList<S2Polygon>();
    do {
      PolygonGeography polygon = readPolygonText(tokenizer, ordinateFlags);
      polygons.add(polygon.polygon);
      nextToken = getNextCloserOrComma(tokenizer);
    } while (nextToken.equals(COMMA));
    return new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, polygons);
  }

  /**
   * Creates a <code>GeometryCollection</code> using the next token in the stream.
   *
   * @param tokenizer tokenizer over a stream of text in Well-known Text format. The next tokens
   *     must form a &lt;GeometryCollection Text&gt;.
   * @return a <code>GeometryCollection</code> specified by the next token in the stream
   * @throws ParseException if the coordinates used to create a <code>Polygon</code> shell and holes
   *     do not form closed linestrings, or if an unexpected token was encountered
   * @throws IOException if an I/O error occurs
   */
  private GeographyCollection readGeographyCollectionText(
      StreamTokenizer tokenizer, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener(tokenizer);
    if (nextToken.equals(WKTConstants.EMPTY)) {
      return new GeographyCollection();
    }
    List<org.apache.sedona.common.S2Geography.Geography> geometries =
        new ArrayList<org.apache.sedona.common.S2Geography.Geography>();
    do {
      org.apache.sedona.common.S2Geography.Geography geometry = readGeometryTaggedText(tokenizer);
      geometries.add(geometry);
      nextToken = getNextCloserOrComma(tokenizer);
    } while (nextToken.equals(COMMA));
    return new GeographyCollection(geometries);
  }
}
