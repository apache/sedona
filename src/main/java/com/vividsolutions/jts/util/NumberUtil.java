package com.vividsolutions.jts.util;

public class NumberUtil
{

  public static boolean equalsWithTolerance(double x1, double x2, double tolerance)
  {
    return Math.abs(x1 - x2) <= tolerance;
  }

}
