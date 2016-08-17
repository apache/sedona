

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
package com.vividsolutions.jts.util;

/**
 *@version 1.7
 */
import java.io.*;
import java.util.*;
import java.lang.reflect.*;
import com.vividsolutions.jts.geom.*;

/**
 * Provides routines to simplify and localize debugging output.
 * Debugging is controlled via a Java system property value.
 * If the system property with the name given in
 * DEBUG_PROPERTY_NAME (currently "jts.debug") has the value
 * "on" or "true" debugging is enabled.
 * Otherwise, debugging is disabled.
 * The system property can be set by specifying the following JVM option:
 * <pre>
 * -Djts.debug=on
 * </pre>
 * 
 *
 * @version 1.7
 */
public class Debug {

  public static String DEBUG_PROPERTY_NAME = "jts.debug";
  public static String DEBUG_PROPERTY_VALUE_ON = "on";
  public static String DEBUG_PROPERTY_VALUE_TRUE = "true";

  private static boolean debugOn = false;

  static {
    String debugValue = System.getProperty(DEBUG_PROPERTY_NAME);
    if (debugValue != null) {
      if (debugValue.equalsIgnoreCase(DEBUG_PROPERTY_VALUE_ON)
          || debugValue.equalsIgnoreCase(DEBUG_PROPERTY_VALUE_TRUE) )
        debugOn = true;
    }
  }

  private static Stopwatch stopwatch = new Stopwatch();
  private static long lastTimePrinted;

  /**
   * Prints the status of debugging to <tt>System.out</tt>
   *
   * @param args the cmd-line arguments (no arguments are required)
   */
  public static void main(String[] args)
  {
    System.out.println("JTS Debugging is " +
                       (debugOn ? "ON" : "OFF") );
  }

  private static final Debug debug = new Debug();
  private static final GeometryFactory fact = new GeometryFactory();
  private static final String DEBUG_LINE_TAG = "D! ";

  private PrintStream out;
  private Class[] printArgs;
  private Object watchObj = null;
  private Object[] args = new Object[1];

  public static boolean isDebugging() { return debugOn; }

  public static LineString toLine(Coordinate p0, Coordinate p1) {
    return fact.createLineString(new Coordinate[] { p0, p1 });
  }

  public static LineString toLine(Coordinate p0, Coordinate p1, Coordinate p2) {
    return fact.createLineString(new Coordinate[] { p0, p1, p2});
  }

  public static LineString toLine(Coordinate p0, Coordinate p1, Coordinate p2, Coordinate p3) {
    return fact.createLineString(new Coordinate[] { p0, p1, p2, p3});
  }

  public static void print(String str) {
    if (!debugOn) {
      return;
    }
    debug.instancePrint(str);
  }
/*
  public static void println(String str) {
    if (! debugOn) return;
    debug.instancePrint(str);
    debug.println();
  }
*/
  public static void print(Object obj) {
    if (! debugOn) return;
    debug.instancePrint(obj);
  }

  public static void print(boolean isTrue, Object obj) {
    if (! debugOn) return;
    if (! isTrue) return;
    debug.instancePrint(obj);
  }

  public static void println(Object obj) {
    if (!debugOn) {
      return;
    }
    debug.instancePrint(obj);
    debug.println();
  }
  
  public static void resetTime()
  {
    stopwatch.reset();
    lastTimePrinted = stopwatch.getTime();
  }
  
  public static void printTime(String tag)
  {
    if (!debugOn) {
      return;
    }
    long time = stopwatch.getTime();
    long elapsedTime = time - lastTimePrinted;
    debug.instancePrint(
        formatField(Stopwatch.getTimeString(time), 10)
        + " (" + formatField(Stopwatch.getTimeString(elapsedTime), 10) + " ) "
        + tag);
    debug.println();    
    lastTimePrinted = time;
  }
  
  private static String formatField(String s, int fieldLen)
  {
    int nPad = fieldLen - s.length();
    if (nPad <= 0) return s;
    String padStr = spaces(nPad) + s;
    return padStr.substring(padStr.length() - fieldLen);
  }
  
  private static String spaces(int n)
  {
    char[] ch = new char[n];
    for (int i = 0; i < n; i++) {
      ch[i] = ' ';
    }
    return new String(ch);
  }
  
  public static boolean equals(Coordinate c1, Coordinate c2, double tolerance)
  {
  	return c1.distance(c2) <= tolerance;
  }
  /**
   * Adds an object to be watched.
   * A watched object can be printed out at any time.
   * 
   * Currently only supports one watched object at a time.
   * @param obj
   */
  public static void addWatch(Object obj) {
    debug.instanceAddWatch(obj);
  }

  public static void printWatch() {
    debug.instancePrintWatch();
  }

  public static void printIfWatch(Object obj) {
    debug.instancePrintIfWatch(obj);
  }

  public static void breakIf(boolean cond)
  {
    if (cond) doBreak();
  }
  
  public static void breakIfEqual(Object o1, Object o2)
  {
    if (o1.equals(o2)) doBreak();
  }
  
  public static void breakIfEqual(Coordinate p0, Coordinate p1, double tolerance)
  {
    if (p0.distance(p1) <= tolerance) doBreak();
  }
  
  private static void doBreak()
  {
    // Put breakpoint on following statement to break here
    return; 
  }
  
  public static boolean hasSegment(Geometry geom, Coordinate p0, Coordinate p1)
  {
    SegmentFindingFilter filter = new SegmentFindingFilter(p0, p1);
    geom.apply(filter);
    return filter.hasSegment();
  }
  
  private static class SegmentFindingFilter
  implements CoordinateSequenceFilter
  {
    private Coordinate p0, p1;
    private boolean hasSegment = false;
    
    public SegmentFindingFilter(Coordinate p0, Coordinate p1)
    {
      this.p0 = p0;
      this.p1 = p1;
    }

    public boolean hasSegment() { return hasSegment; }

    public void filter(CoordinateSequence seq, int i)
    {
      if (i == 0) return;
      hasSegment = p0.equals2D(seq.getCoordinate(i-1)) 
          && p1.equals2D(seq.getCoordinate(i));
    }
    
    public boolean isDone()
    {
      return hasSegment; 
    }
    
    public boolean isGeometryChanged()
    {
      return false;
    }
  }
  
  private Debug() {
    out = System.out;
    printArgs = new Class[1];
    try {
      printArgs[0] = Class.forName("java.io.PrintStream");
    }
    catch (Exception ex) {
      // ignore this exception - it will fail later anyway
    }
  }

  public void instancePrintWatch() {
    if (watchObj == null) return;
    instancePrint(watchObj);
  }

  public void instancePrintIfWatch(Object obj) {
    if (obj != watchObj) return;
    if (watchObj == null) return;
    instancePrint(watchObj);
  }

  public void instancePrint(Object obj)
  {
    if (obj instanceof Collection) {
      instancePrint(((Collection) obj).iterator());
    }
    else if (obj instanceof Iterator) {
      instancePrint((Iterator) obj);
    }
    else {
      instancePrintObject(obj);
    }
  }

  public void instancePrint(Iterator it)
  {
    for (; it.hasNext(); ) {
      Object obj = it.next();
      instancePrintObject(obj);
    }
  }
  public void instancePrintObject(Object obj) {
    //if (true) throw new RuntimeException("DEBUG TRAP!");
    Method printMethod = null;
    try {
      Class cls = obj.getClass();
      try {
        printMethod = cls.getMethod("print", printArgs);
        args[0] = out;
        out.print(DEBUG_LINE_TAG);
        printMethod.invoke(obj, args);
      }
      catch (NoSuchMethodException ex) {
        instancePrint(obj.toString());
      }
    }
    catch (Exception ex) {
      ex.printStackTrace(out);
    }
  }

  public void println() {
    out.println();
  }

  private void instanceAddWatch(Object obj) {
    watchObj = obj;
  }

  private void instancePrint(String str) {
    out.print(DEBUG_LINE_TAG);
    out.print(str);
  }

}
