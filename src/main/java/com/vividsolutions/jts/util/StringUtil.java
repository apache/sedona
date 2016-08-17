package com.vividsolutions.jts.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;

/**
 * Utility methods for working with {@link String}s.
 * 
 * @author Martin Davis
 *
 */
public class StringUtil
{
  /**
   * Mimics the the Java SE {@link String#split(String)} method.
   *
   * @param s the string to split.
   * @param separator the separator to use.
   * @return the array of split strings.
   */
 public static String[] split(String s, String separator)
 {
   int separatorlen = separator.length();
   ArrayList tokenList = new ArrayList();
   String tmpString = "" + s;
   int pos = tmpString.indexOf(separator);
   while (pos >= 0) {
     String token = tmpString.substring(0, pos);
     tokenList.add(token);
     tmpString = tmpString.substring(pos + separatorlen);
     pos = tmpString.indexOf(separator);
   }
   if (tmpString.length() > 0)
     tokenList.add(tmpString);
   String[] res = new String[tokenList.size()];
   for (int i = 0; i < res.length; i++) {
     res[i] = (String) tokenList.get(i);
   }
   return res;
 }

 public final static String NEWLINE = System.getProperty("line.separator");

 /**
  *  Returns an throwable's stack trace
  */
 public static String getStackTrace(Throwable t) {
     ByteArrayOutputStream os = new ByteArrayOutputStream();
     PrintStream ps = new PrintStream(os);
     t.printStackTrace(ps);
     return os.toString();
 }

 public static String getStackTrace(Throwable t, int depth) {
     String stackTrace = "";
     StringReader stringReader = new StringReader(getStackTrace(t));
     LineNumberReader lineNumberReader = new LineNumberReader(stringReader);
     for (int i = 0; i < depth; i++) {
         try {
             stackTrace += lineNumberReader.readLine() + NEWLINE;
         } catch (IOException e) {
             Assert.shouldNeverReachHere();
         }
     }
     return stackTrace;
 }

  private static NumberFormat SIMPLE_ORDINATE_FORMAT = new DecimalFormat("0.#");
  
  public static String toString(double d)
  {
    return SIMPLE_ORDINATE_FORMAT.format(d);
  }

  public static String spaces(int n)
  {
    return chars(' ', n);
  }
  
  public static String chars(char c, int n)
  {
    char[] ch = new char[n];
    for (int i = 0; i < n; i++) {
      ch[i] = c;
    }
    return new String(ch);
  }
}
