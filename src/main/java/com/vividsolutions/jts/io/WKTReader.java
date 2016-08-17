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
import com.vividsolutions.jts.io.ParseException;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;

/**
 * Converts a geometry in Well-Known Text format to a {@link Geometry}.
 * <p>
 * <code>WKTReader</code> supports
 * extracting <code>Geometry</code> objects from either {@link Reader}s or
 *  {@link String}s. This allows it to function as a parser to read <code>Geometry</code>
 *  objects from text blocks embedded in other data formats (e.g. XML). <P>
 * <p>
 *  A <code>WKTReader</code> is parameterized by a <code>GeometryFactory</code>,
 *  to allow it to create <code>Geometry</code> objects of the appropriate
 *  implementation. In particular, the <code>GeometryFactory</code>
 *  determines the <code>PrecisionModel</code> and <code>SRID</code> that is
 *  used. <P>
 *
 *  The <code>WKTReader</code> converts all input numbers to the precise
 *  internal representation.
 *
 * <h3>Notes:</h3>
 * <ul>
 * <li>Keywords are case-insensitive.
 * <li>The reader supports non-standard "LINEARRING" tags.
 * <li>The reader uses <tt>Double.parseDouble</tt> to perform the conversion of ASCII
 * numbers to floating point.  This means it supports the Java
 * syntax for floating point literals (including scientific notation).
 * </ul>
 *
 * <h3>Syntax</h3>
 * The following syntax specification describes the version of Well-Known Text
 * supported by JTS.
 * (The specification uses a syntax language similar to that used in
 * the C and Java language specifications.)
 * <p>
 *
 * <blockquote><pre>
 * <i>WKTGeometry:</i> one of<i>
 *
 *       WKTPoint  WKTLineString  WKTLinearRing  WKTPolygon
 *       WKTMultiPoint  WKTMultiLineString  WKTMultiPolygon
 *       WKTGeometryCollection</i>
 *
 * <i>WKTPoint:</i> <b>POINT ( </b><i>Coordinate</i> <b>)</b>
 *
 * <i>WKTLineString:</i> <b>LINESTRING</b> <i>CoordinateSequence</i>
 *
 * <i>WKTLinearRing:</i> <b>LINEARRING</b> <i>CoordinateSequence</i>
 *
 * <i>WKTPolygon:</i> <b>POLYGON</b> <i>CoordinateSequenceList</i>
 *
 * <i>WKTMultiPoint:</i> <b>MULTIPOINT</b> <i>CoordinateSingletonList</i>
 *
 * <i>WKTMultiLineString:</i> <b>MULTILINESTRING</b> <i>CoordinateSequenceList</i>
 *
 * <i>WKTMultiPolygon:</i>
 *         <b>MULTIPOLYGON (</b> <i>CoordinateSequenceList {</i> , <i>CoordinateSequenceList }</i> <b>)</b>
 *
 * <i>WKTGeometryCollection: </i>
 *         <b>GEOMETRYCOLLECTION (</b> <i>WKTGeometry {</i> , <i>WKTGeometry }</i> <b>)</b>
 *
 * <i>CoordinateSingletonList:</i>
 *         <b>(</b> <i>CoordinateSingleton {</i> <b>,</b> <i>CoordinateSingleton }</i> <b>)</b>
 *         | <b>EMPTY</b>
 *         
 * <i>CoordinateSingleton:</i>
 *         <b>(</b> <i>Coordinate <b>)</b>
 *         | <b>EMPTY</b>
 *
 * <i>CoordinateSequenceList:</i>
 *         <b>(</b> <i>CoordinateSequence {</i> <b>,</b> <i>CoordinateSequence }</i> <b>)</b>
 *         | <b>EMPTY</b>
 *
 * <i>CoordinateSequence:</i>
 *         <b>(</b> <i>Coordinate {</i> , <i>Coordinate }</i> <b>)</b>
 *         | <b>EMPTY</b>
 *
 * <i>Coordinate:
 *         Number Number Number<sub>opt</sub></i>
 *
 * <i>Number:</i> A Java-style floating-point number (including <tt>NaN</tt>, with arbitrary case)
 *
 * </pre></blockquote>
 *
 *
 *@version 1.7
 * @see WKTWriter
 */
public class WKTReader
{
  private static final String EMPTY = "EMPTY";
  private static final String COMMA = ",";
  private static final String L_PAREN = "(";
  private static final String R_PAREN = ")";
  private static final String NAN_SYMBOL = "NaN";
  
  private GeometryFactory geometryFactory;
  private PrecisionModel precisionModel;
  private StreamTokenizer tokenizer;

  /**
   * Creates a reader that creates objects using the default {@link GeometryFactory}.
   */
  public WKTReader() {
    this(new GeometryFactory());
  }

  /**
   *  Creates a reader that creates objects using the given
   *  {@link GeometryFactory}.
   *
   *@param  geometryFactory  the factory used to create <code>Geometry</code>s.
   */
  public WKTReader(GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
    precisionModel = geometryFactory.getPrecisionModel();
  }

  /**
   * Reads a Well-Known Text representation of a {@link Geometry}
   * from a {@link String}.
   *
   * @param wellKnownText
   *            one or more <Geometry Tagged Text>strings (see the OpenGIS
   *            Simple Features Specification) separated by whitespace
   * @return a <code>Geometry</code> specified by <code>wellKnownText</code>
   * @throws ParseException
   *             if a parsing problem occurs
   */
  public Geometry read(String wellKnownText) throws ParseException {
    StringReader reader = new StringReader(wellKnownText);
    try {
      return read(reader);
    }
    finally {
      reader.close();
    }
  }

  /**
   * Reads a Well-Known Text representation of a {@link Geometry}
   * from a {@link Reader}.
   *
   *@param  reader           a Reader which will return a <Geometry Tagged Text>
   *      string (see the OpenGIS Simple Features Specification)
   *@return                  a <code>Geometry</code> read from <code>reader</code>
   *@throws  ParseException  if a parsing problem occurs
   */
  public Geometry read(Reader reader) throws ParseException {
    tokenizer = new StreamTokenizer(reader);
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

    try {
      return readGeometryTaggedText();
    }
    catch (IOException e) {
      throw new ParseException(e.toString());
    }
  }

  /**
   * Returns the next array of <code>Coordinate</code>s in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next element returned by the stream should be L_PAREN (the
   *      beginning of "(x1 y1, x2 y2, ..., xn yn)") or EMPTY.
   *@return                  the next array of <code>Coordinate</code>s in the
   *      stream, or an empty array if EMPTY is the next element returned by
   *      the stream.
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if an unexpected token was encountered
   */
  private Coordinate[] getCoordinates() throws IOException, ParseException {
		String nextToken = getNextEmptyOrOpener();
		if (nextToken.equals(EMPTY)) {
			return new Coordinate[] {};
		}
		ArrayList coordinates = new ArrayList();
		coordinates.add(getPreciseCoordinate());
		nextToken = getNextCloserOrComma();
		while (nextToken.equals(COMMA)) {
			coordinates.add(getPreciseCoordinate());
			nextToken = getNextCloserOrComma();
		}
		Coordinate[] array = new Coordinate[coordinates.size()];
		return (Coordinate[]) coordinates.toArray(array);
	}

	private Coordinate[] getCoordinatesNoLeftParen() throws IOException, ParseException {
		String nextToken = null;
		ArrayList coordinates = new ArrayList();
		coordinates.add(getPreciseCoordinate());
		nextToken = getNextCloserOrComma();
		while (nextToken.equals(COMMA)) {
			coordinates.add(getPreciseCoordinate());
			nextToken = getNextCloserOrComma();
		}
		Coordinate[] array = new Coordinate[coordinates.size()];
		return (Coordinate[]) coordinates.toArray(array);
	}

  private Coordinate getPreciseCoordinate()
      throws IOException, ParseException
  {
    Coordinate coord = new Coordinate();
    coord.x = getNextNumber();
    coord.y = getNextNumber();
    if (isNumberNext()) {
        coord.z = getNextNumber();
    }
    precisionModel.makePrecise(coord);
    return coord;
  }

  private boolean isNumberNext() throws IOException {
    int type = tokenizer.nextToken();
    tokenizer.pushBack();
    return type == StreamTokenizer.TT_WORD;
  }

  /**
   * Parses the next number in the stream.
   * Numbers with exponents are handled.
   * <tt>NaN</tt> values are handled correctly, and
   * the case of the "NaN" symbol is not significant. 
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next token must be a number.
   *@return                  the next number in the stream
   *@throws  ParseException  if the next token is not a valid number
   *@throws  IOException     if an I/O error occurs
   */
  private double getNextNumber() throws IOException,
      ParseException {
    int type = tokenizer.nextToken();
    switch (type) {
      case StreamTokenizer.TT_WORD:
      {
      	if (tokenizer.sval.equalsIgnoreCase(NAN_SYMBOL)) {
      		return Double.NaN;
      	}
      	else {
	        try {
	          return Double.parseDouble(tokenizer.sval);
	        }
	        catch (NumberFormatException ex) {
	          parseErrorWithLine("Invalid number: " + tokenizer.sval);
	        }
      	}
      }
    }
    parseErrorExpected("number");
    return 0.0;
  }
  /**
   *  Returns the next EMPTY or L_PAREN in the stream as uppercase text.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next token must be EMPTY or L_PAREN.
   *@return                  the next EMPTY or L_PAREN in the stream as uppercase
   *      text.
   *@throws  ParseException  if the next token is not EMPTY or L_PAREN
   *@throws  IOException     if an I/O error occurs
   */
  private String getNextEmptyOrOpener() throws IOException, ParseException {
    String nextWord = getNextWord();
    if (nextWord.equals(EMPTY) || nextWord.equals(L_PAREN)) {
      return nextWord;
    }
    parseErrorExpected(EMPTY + " or " + L_PAREN);
    return null;
  }

  /**
   *  Returns the next R_PAREN or COMMA in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next token must be R_PAREN or COMMA.
   *@return                  the next R_PAREN or COMMA in the stream
   *@throws  ParseException  if the next token is not R_PAREN or COMMA
   *@throws  IOException     if an I/O error occurs
   */
  private String getNextCloserOrComma() throws IOException, ParseException {
    String nextWord = getNextWord();
    if (nextWord.equals(COMMA) || nextWord.equals(R_PAREN)) {
      return nextWord;
    }
    parseErrorExpected(COMMA + " or " + R_PAREN);
    return null;
  }

  /**
   *  Returns the next R_PAREN in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next token must be R_PAREN.
   *@return                  the next R_PAREN in the stream
   *@throws  ParseException  if the next token is not R_PAREN
   *@throws  IOException     if an I/O error occurs
   */
  private String getNextCloser() throws IOException, ParseException {
    String nextWord = getNextWord();
    if (nextWord.equals(R_PAREN)) {
      return nextWord;
    }
    parseErrorExpected(R_PAREN);
    return null;
  }

  /**
   *  Returns the next word in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next token must be a word.
   *@return                  the next word in the stream as uppercase text
   *@throws  ParseException  if the next token is not a word
   *@throws  IOException     if an I/O error occurs
   */
  private String getNextWord() throws IOException, ParseException {
    int type = tokenizer.nextToken();
    switch (type) {
    case StreamTokenizer.TT_WORD:

      String word = tokenizer.sval;
      if (word.equalsIgnoreCase(EMPTY))
          return EMPTY;
      return word;

    case '(': return L_PAREN;
    case ')': return R_PAREN;
    case ',': return COMMA;
    }
    parseErrorExpected("word");
    return null;
  }

  /**
   *  Returns the next word in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next token must be a word.
   *@return                  the next word in the stream as uppercase text
   *@throws  ParseException  if the next token is not a word
   *@throws  IOException     if an I/O error occurs
   */
  private String lookaheadWord() throws IOException, ParseException {
  	String nextWord = getNextWord();
  	tokenizer.pushBack();
    return nextWord;
  }

  /**
   * Throws a formatted ParseException reporting that the current token
   * was unexpected.
   *
   * @param expected a description of what was expected
   * @throws ParseException
   * @throws AssertionFailedException if an invalid token is encountered
   */
  private void parseErrorExpected(String expected)
      throws ParseException
  {
    // throws Asserts for tokens that should never be seen
    if (tokenizer.ttype == StreamTokenizer.TT_NUMBER)
      Assert.shouldNeverReachHere("Unexpected NUMBER token");
    if (tokenizer.ttype == StreamTokenizer.TT_EOL)
      Assert.shouldNeverReachHere("Unexpected EOL token");

    String tokenStr = tokenString();
    parseErrorWithLine("Expected " + expected + " but found " + tokenStr);
  }

  private void parseErrorWithLine(String msg)
  throws ParseException
  {
    throw new ParseException(msg + " (line " + tokenizer.lineno() + ")");
  }
  
  /**
   * Gets a description of the current token
   *
   * @return a description of the current token
   */
  private String tokenString()
  {
    switch (tokenizer.ttype) {
      case StreamTokenizer.TT_NUMBER:
        return "<NUMBER>";
      case StreamTokenizer.TT_EOL:
        return "End-of-Line";
      case StreamTokenizer.TT_EOF: return "End-of-Stream";
      case StreamTokenizer.TT_WORD: return "'" + tokenizer.sval + "'";
    }
    return "'" + (char) tokenizer.ttype + "'";
  }

  /**
   *  Creates a <code>Geometry</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;Geometry Tagged Text&gt;.
   *@return                  a <code>Geometry</code> specified by the next token
   *      in the stream
   *@throws  ParseException  if the coordinates used to create a <code>Polygon</code>
   *      shell and holes do not form closed linestrings, or if an unexpected
   *      token was encountered
   *@throws  IOException     if an I/O error occurs
   */
  private Geometry readGeometryTaggedText() throws IOException, ParseException {
    String type = null;
    
    try{
    	type = getNextWord();
    }catch(IOException e){
    	return null;
    }catch(ParseException e){
    	return null;
    }
    
    if (type.equalsIgnoreCase("POINT")) {
      return readPointText();
    }
    else if (type.equalsIgnoreCase("LINESTRING")) {
      return readLineStringText();
    }
    else if (type.equalsIgnoreCase("LINEARRING")) {
      return readLinearRingText();
    }
    else if (type.equalsIgnoreCase("POLYGON")) {
      return readPolygonText();
    }
    else if (type.equalsIgnoreCase("MULTIPOINT")) {
      return readMultiPointText();
    }
    else if (type.equalsIgnoreCase("MULTILINESTRING")) {
      return readMultiLineStringText();
    }
    else if (type.equalsIgnoreCase("MULTIPOLYGON")) {
      return readMultiPolygonText();
    }
    else if (type.equalsIgnoreCase("GEOMETRYCOLLECTION")) {
      return readGeometryCollectionText();
    }
    parseErrorWithLine("Unknown geometry type: " + type);
    // should never reach here
    return null;
  }

  /**
   *  Creates a <code>Point</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;Point Text&gt;.
   *@return                  a <code>Point</code> specified by the next token in
   *      the stream
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if an unexpected token was encountered
   */
  private Point readPointText() throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener();
    if (nextToken.equals(EMPTY)) {
      return geometryFactory.createPoint((Coordinate)null);
    }
    Point point = geometryFactory.createPoint(getPreciseCoordinate());
    getNextCloser();
    return point;
  }

  /**
   *  Creates a <code>LineString</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;LineString Text&gt;.
   *@return                  a <code>LineString</code> specified by the next
   *      token in the stream
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if an unexpected token was encountered
   */
  private LineString readLineStringText() throws IOException, ParseException {
    return geometryFactory.createLineString(getCoordinates());
  }

  /**
   *  Creates a <code>LinearRing</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;LineString Text&gt;.
   *@return                  a <code>LinearRing</code> specified by the next
   *      token in the stream
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if the coordinates used to create the <code>LinearRing</code>
   *      do not form a closed linestring, or if an unexpected token was
   *      encountered
   */
  private LinearRing readLinearRingText()
    throws IOException, ParseException
  {
    return geometryFactory.createLinearRing(getCoordinates());
  }

  /*
  private MultiPoint OLDreadMultiPointText() throws IOException, ParseException {
    return geometryFactory.createMultiPoint(toPoints(getCoordinates()));
  }
  */
  
  private static final boolean ALLOW_OLD_JTS_MULTIPOINT_SYNTAX = true;

  /**
   *  Creates a <code>MultiPoint</code> using the next tokens in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;MultiPoint Text&gt;.
   *@return                  a <code>MultiPoint</code> specified by the next
   *      token in the stream
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if an unexpected token was encountered
   */
  private MultiPoint readMultiPointText() throws IOException, ParseException 
  {
    String nextToken = getNextEmptyOrOpener();
    if (nextToken.equals(EMPTY)) {
      return geometryFactory.createMultiPoint(new Point[0]);
    }
    
    // check for old-style JTS syntax and parse it if present
  	// MD 2009-02-21 - this is only provided for backwards compatibility for a few versions
  	if (ALLOW_OLD_JTS_MULTIPOINT_SYNTAX) {
  		String nextWord = lookaheadWord();
  		if (nextWord != L_PAREN) {
  			return geometryFactory.createMultiPoint(toPoints(getCoordinatesNoLeftParen()));
  		}
  	}
  	
    ArrayList points = new ArrayList();
    Point point = readPointText();
    points.add(point);
    nextToken = getNextCloserOrComma();
    while (nextToken.equals(COMMA)) {
    	point = readPointText();
      points.add(point);
      nextToken = getNextCloserOrComma();
    }
    Point[] array = new Point[points.size()];
    return geometryFactory.createMultiPoint((Point[]) points.toArray(array));
  }

  /**
   *  Creates an array of <code>Point</code>s having the given <code>Coordinate</code>
   *  s.
   *
   *@param  coordinates  the <code>Coordinate</code>s with which to create the
   *      <code>Point</code>s
   *@return              <code>Point</code>s created using this <code>WKTReader</code>
   *      s <code>GeometryFactory</code>
   */
  private Point[] toPoints(Coordinate[] coordinates) {
    ArrayList points = new ArrayList();
    for (int i = 0; i < coordinates.length; i++) {
      points.add(geometryFactory.createPoint(coordinates[i]));
    }
    return (Point[]) points.toArray(new Point[]{});
  }

  /**
   *  Creates a <code>Polygon</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;Polygon Text&gt;.
   *@return                  a <code>Polygon</code> specified by the next token
   *      in the stream
   *@throws  ParseException  if the coordinates used to create the <code>Polygon</code>
   *      shell and holes do not form closed linestrings, or if an unexpected
   *      token was encountered.
   *@throws  IOException     if an I/O error occurs
   */
  private Polygon readPolygonText() throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener();
    if (nextToken.equals(EMPTY)) {
        return geometryFactory.createPolygon(geometryFactory.createLinearRing(
            new Coordinate[]{}), new LinearRing[]{});
    }
    ArrayList holes = new ArrayList();
    LinearRing shell = readLinearRingText();
    nextToken = getNextCloserOrComma();
    while (nextToken.equals(COMMA)) {
      LinearRing hole = readLinearRingText();
      holes.add(hole);
      nextToken = getNextCloserOrComma();
    }
    LinearRing[] array = new LinearRing[holes.size()];
    return geometryFactory.createPolygon(shell, (LinearRing[]) holes.toArray(array));
  }

  /**
   *  Creates a <code>MultiLineString</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;MultiLineString Text&gt;.
   *@return                  a <code>MultiLineString</code> specified by the
   *      next token in the stream
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if an unexpected token was encountered
   */
  private com.vividsolutions.jts.geom.MultiLineString readMultiLineStringText() throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener();
    if (nextToken.equals(EMPTY)) {
      return geometryFactory.createMultiLineString(new LineString[]{});
    }
    ArrayList lineStrings = new ArrayList();
    LineString lineString = readLineStringText();
    lineStrings.add(lineString);
    nextToken = getNextCloserOrComma();
    while (nextToken.equals(COMMA)) {
      lineString = readLineStringText();
      lineStrings.add(lineString);
      nextToken = getNextCloserOrComma();
    }
    LineString[] array = new LineString[lineStrings.size()];
    return geometryFactory.createMultiLineString((LineString[]) lineStrings.toArray(array));
  }

  /**
   *  Creates a <code>MultiPolygon</code> using the next token in the stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;MultiPolygon Text&gt;.
   *@return                  a <code>MultiPolygon</code> specified by the next
   *      token in the stream, or if if the coordinates used to create the
   *      <code>Polygon</code> shells and holes do not form closed linestrings.
   *@throws  IOException     if an I/O error occurs
   *@throws  ParseException  if an unexpected token was encountered
   */
  private MultiPolygon readMultiPolygonText() throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener();
    if (nextToken.equals(EMPTY)) {
      return geometryFactory.createMultiPolygon(new Polygon[]{});
    }
    ArrayList polygons = new ArrayList();
    Polygon polygon = readPolygonText();
    polygons.add(polygon);
    nextToken = getNextCloserOrComma();
    while (nextToken.equals(COMMA)) {
      polygon = readPolygonText();
      polygons.add(polygon);
      nextToken = getNextCloserOrComma();
    }
    Polygon[] array = new Polygon[polygons.size()];
    return geometryFactory.createMultiPolygon((Polygon[]) polygons.toArray(array));
  }

  /**
   *  Creates a <code>GeometryCollection</code> using the next token in the
   *  stream.
   *
   *@param  tokenizer        tokenizer over a stream of text in Well-known Text
   *      format. The next tokens must form a &lt;GeometryCollection Text&gt;.
   *@return                  a <code>GeometryCollection</code> specified by the
   *      next token in the stream
   *@throws  ParseException  if the coordinates used to create a <code>Polygon</code>
   *      shell and holes do not form closed linestrings, or if an unexpected
   *      token was encountered
   *@throws  IOException     if an I/O error occurs
   */
  private GeometryCollection readGeometryCollectionText() throws IOException, ParseException {
    String nextToken = getNextEmptyOrOpener();
    if (nextToken.equals(EMPTY)) {
      return geometryFactory.createGeometryCollection(new Geometry[]{});
    }
    ArrayList geometries = new ArrayList();
    Geometry geometry = readGeometryTaggedText();
    geometries.add(geometry);
    nextToken = getNextCloserOrComma();
    while (nextToken.equals(COMMA)) {
      geometry = readGeometryTaggedText();
      geometries.add(geometry);
      nextToken = getNextCloserOrComma();
    }
    Geometry[] array = new Geometry[geometries.size()];
    return geometryFactory.createGeometryCollection((Geometry[]) geometries.toArray(array));
  }

}

