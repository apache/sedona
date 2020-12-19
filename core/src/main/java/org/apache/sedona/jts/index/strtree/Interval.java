
/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.apache.sedona.jts.index.strtree;

import org.locationtech.jts.util.Assert;

/**
 * A contiguous portion of 1D-space. Used internally by SIRtree.
 * @see SIRtree
 *
 * @version 1.7
 */
public class Interval {

  public Interval(Interval other) {
    this(other.min, other.max);
  }

  public Interval(double min, double max) {
    Assert.isTrue(min <= max);
    this.min = min;
    this.max = max;
  }

  private double min;
  private double max;

  public double getCentre() { return (min+max)/2; }

  /**
   * @return this
   */
  public Interval expandToInclude(Interval other) {
    max = Math.max(max, other.max);
    min = Math.min(min, other.min);
    return this;
  }

  public boolean intersects(Interval other) {
    return !(other.min > max || other.max < min);
  }
  public boolean equals(Object o) {
    if (! (o instanceof Interval)) { return false; }
    Interval other = (Interval) o;
    return min == other.min && max == other.max;
  }
}
