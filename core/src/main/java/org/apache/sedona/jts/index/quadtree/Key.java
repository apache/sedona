
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
package org.apache.sedona.jts.index.quadtree;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

/**
 * A Key is a unique identifier for a node in a quadtree.
 * It contains a lower-left point and a level number. The level number
 * is the power of two for the size of the node envelope
 *
 * @version 1.7
 */
public class Key {

  public static int computeQuadLevel(Envelope env)
  {
    double dx = env.getWidth();
    double dy = env.getHeight();
    double dMax = dx > dy ? dx : dy;
    int level = DoubleBits.exponent(dMax) + 1;
    return level;
  }

  // the fields which make up the key
  private Coordinate pt = new Coordinate();
  private int level = 0;
  // auxiliary data which is derived from the key for use in computation
  private Envelope env = null;

  public Key(Envelope itemEnv)
  {
    computeKey(itemEnv);
  }

  public Coordinate getPoint() { return pt; }
  public int getLevel() { return level; }
  public Envelope getEnvelope() { return env; }

  public Coordinate getCentre()
  {
    return new Coordinate(
      (env.getMinX() + env.getMaxX()) / 2,
      (env.getMinY() + env.getMaxY()) / 2
      );
  }
  /**
   * return a square envelope containing the argument envelope,
   * whose extent is a power of two and which is based at a power of 2
   */
  public void computeKey(Envelope itemEnv)
  {
    level = computeQuadLevel(itemEnv);
    env = new Envelope();
    computeKey(level, itemEnv);
    // MD - would be nice to have a non-iterative form of this algorithm
    while (! env.contains(itemEnv)) {
      level += 1;
      computeKey(level, itemEnv);
    }
  }

  private void computeKey(int level, Envelope itemEnv)
  {
    double quadSize = DoubleBits.powerOf2(level);
    pt.x = Math.floor(itemEnv.getMinX() / quadSize) * quadSize;
    pt.y = Math.floor(itemEnv.getMinY() / quadSize) * quadSize;
    env.init(pt.x, pt.x + quadSize, pt.y, pt.y + quadSize);
  }
}
