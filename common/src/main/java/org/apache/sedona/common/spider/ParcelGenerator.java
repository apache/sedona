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
package org.apache.sedona.common.spider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/** Generate boxes that are non-overlapping and fill up the unit square. */
public class ParcelGenerator implements Generator {

  public static class ParcelParameter {
    /** The number of boxes to generate */
    public final long cardinality;

    /** The amount of dithering as a ratio of the side length. Allowed range [0, 1] */
    public final double dither;

    /**
     * The allowed range for splitting boxes. Allowed range [0.0, 0.5] 0.0 means all values are
     * allowed. 0.5 means always split in half.
     */
    public final double splitRange;

    public ParcelParameter(long cardinality, double dither, double splitRange) {
      if (cardinality < 0) {
        throw new IllegalArgumentException("cardinality must be non-negative");
      }
      if (dither < 0 || dither > 1) {
        throw new IllegalArgumentException("dither must be in the range [0, 1]");
      }
      if (splitRange < 0 || splitRange > 0.5) {
        throw new IllegalArgumentException("splitRange must be in the range [0, 0.5]");
      }
      this.cardinality = cardinality;
      this.dither = dither;
      this.splitRange = splitRange;
    }

    public static ParcelParameter create(Map<String, String> conf) {
      long cardinality = Long.parseLong(conf.getOrDefault("cardinality", "100"));
      double dither = Double.parseDouble(conf.getOrDefault("dither", "0.5"));
      double splitRange = Double.parseDouble(conf.getOrDefault("splitRange", "0.5"));
      return new ParcelParameter(cardinality, dither, splitRange);
    }
  }

  private final Random random;

  private long iRecord = 0;
  private final long cardinality;
  private final double splitRange;
  private final double dither;

  private static class BoxWithLevel {
    public final Envelope envelope;
    public final int level;

    public BoxWithLevel(Envelope envelope, int level) {
      this.envelope = envelope;
      this.level = level;
    }
  }

  /** A stack of boxes to split. Each pair represents the level and the box */
  private List<BoxWithLevel> boxesToSplit;

  /**
   * The level of the deepest box to generate is &lceil; log<sub>2</sub>(n)&rceil; =
   * &lfloor;log<sub>2</sub>(n-1)&rfloor; + 1
   */
  private final int maxDepth;

  /**
   * Number of boxes that will be generated at the deepest level (maxDepth). The remaining records
   * will be generated at level maxDepth - 1
   */
  private final long numBoxesMaxDepth;

  public ParcelGenerator(Random random, ParcelParameter parameter) {
    this.random = random;

    this.cardinality = parameter.cardinality;
    this.splitRange = parameter.splitRange;
    this.dither = parameter.dither;

    this.boxesToSplit = new ArrayList<>();
    this.boxesToSplit.add(new BoxWithLevel(new Envelope(0, 1, 0, 1), 0));

    this.maxDepth = 64 - Long.numberOfLeadingZeros(cardinality - 1);
    this.numBoxesMaxDepth = 2 * cardinality - (1L << maxDepth);
  }

  /** Generates a box by first generating a point and building a box around it */
  public Envelope generateBox() {
    assert !boxesToSplit.isEmpty();
    assert iRecord < cardinality;
    BoxWithLevel boxWithLevel = boxesToSplit.remove(boxesToSplit.size() - 1);
    int level = boxWithLevel.level;
    Envelope box = boxWithLevel.envelope;

    while (true) {
      if (level == maxDepth || (level == maxDepth - 1 && iRecord >= numBoxesMaxDepth)) {
        // Box is final. Return it
        ditherBox(box);
        iRecord += 1;
        return box;
      } else {
        // Split the box into two
        Envelope[] splitBoxes = splitBox(box);
        boxesToSplit.add(new BoxWithLevel(splitBoxes[1], level + 1));
        // Update the level and box for the next iteration
        level = level + 1;
        box = splitBoxes[0];
      }
    }
  }

  /**
   * Split the given box into two according to the splitRange value. This function always splits the
   * box along the longest side. Let's assume the longest side has a length l, the split will happen
   * at l * uniform(splitRange, 1-splitRange).
   */
  private Envelope[] splitBox(Envelope box) {
    boolean splitX = box.getWidth() > box.getHeight();
    if (splitX) {
      double splitPoint =
          box.getMinX()
              + box.getWidth() * (splitRange + random.nextDouble() * (1 - 2 * splitRange));
      return new Envelope[] {
        new Envelope(box.getMinX(), splitPoint, box.getMinY(), box.getMaxY()),
        new Envelope(splitPoint, box.getMaxX(), box.getMinY(), box.getMaxY())
      };
    } else {
      double splitPoint =
          box.getMinY()
              + box.getHeight() * (splitRange + random.nextDouble() * (1 - 2 * splitRange));
      return new Envelope[] {
        new Envelope(box.getMinX(), box.getMaxX(), box.getMinY(), splitPoint),
        new Envelope(box.getMinX(), box.getMaxX(), splitPoint, box.getMaxY())
      };
    }
  }

  /**
   * Change the size of the given box along all dimensions according to the dither parameter. The
   * amount of change on the side length is a uniformly random variable between [0, dither). This
   * means that if the dither parameter is zero, the box will not be changed. The center of the box
   * remains fixed while dither
   */
  private void ditherBox(Envelope box) {
    double changeX = random.nextDouble() * dither * box.getWidth();
    double changeY = random.nextDouble() * dither * box.getHeight();

    box.init(
        box.getMinX() + changeX / 2,
        box.getMaxX() - changeX / 2,
        box.getMinY() + changeY / 2,
        box.getMaxY() - changeY / 2);
  }

  @Override
  public boolean hasNext() {
    return iRecord < cardinality;
  }

  @Override
  public Geometry next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more parcels to generate");
    }
    return GEOMETRY_FACTORY.toGeometry(generateBox());
  }
}
