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
package org.apache.sedona.common.raster.cog;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Options for Cloud Optimized GeoTIFF (COG) generation.
 *
 * <p>Use the {@link Builder} to construct instances:
 *
 * <pre>{@code
 * CogOptions opts = CogOptions.builder()
 *     .compression("LZW")
 *     .compressionQuality(0.5)
 *     .tileSize(512)
 *     .resampling("Bilinear")
 *     .overviewCount(3)
 *     .build();
 * }</pre>
 *
 * <p>All fields are immutable once constructed. Validation is performed in {@link Builder#build()}.
 */
public final class CogOptions {

  /** Supported compression algorithms. */
  private static final List<String> VALID_COMPRESSION =
      Arrays.asList("Deflate", "LZW", "JPEG", "PackBits");

  /** Supported resampling algorithms for overview generation. */
  private static final List<String> VALID_RESAMPLING =
      Arrays.asList("Nearest", "Bilinear", "Bicubic");

  private final String compression;
  private final double compressionQuality;
  private final int tileSize;
  private final String resampling;
  private final int overviewCount;

  private CogOptions(Builder builder) {
    this.compression = builder.compression;
    this.compressionQuality = builder.compressionQuality;
    this.tileSize = builder.tileSize;
    this.resampling = builder.resampling;
    this.overviewCount = builder.overviewCount;
  }

  /**
   * @return Compression type: "Deflate", "LZW", "JPEG", "PackBits"
   */
  public String getCompression() {
    return compression;
  }

  /**
   * @return Compression quality from 0.0 (max compression) to 1.0 (no compression)
   */
  public double getCompressionQuality() {
    return compressionQuality;
  }

  /**
   * @return Tile width and height in pixels (always a power of 2)
   */
  public int getTileSize() {
    return tileSize;
  }

  /**
   * @return Resampling algorithm for overview generation: "Nearest", "Bilinear", or "Bicubic"
   */
  public String getResampling() {
    return resampling;
  }

  /**
   * @return Number of overview levels. -1 means auto-compute based on image dimensions, 0 means no
   *     overviews.
   */
  public int getOverviewCount() {
    return overviewCount;
  }

  /**
   * @return A new builder initialized with default values
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The default options (Deflate, quality 0.2, 256px tiles, Nearest, auto overviews)
   */
  public static CogOptions defaults() {
    return new Builder().build();
  }

  @Override
  public String toString() {
    return "CogOptions{"
        + "compression='"
        + compression
        + '\''
        + ", compressionQuality="
        + compressionQuality
        + ", tileSize="
        + tileSize
        + ", resampling='"
        + resampling
        + '\''
        + ", overviewCount="
        + overviewCount
        + '}';
  }

  /** Builder for {@link CogOptions}. */
  public static final class Builder {
    private String compression = "Deflate";
    private double compressionQuality = 0.2;
    private int tileSize = 256;
    private String resampling = "Nearest";
    private int overviewCount = -1;

    private Builder() {}

    /**
     * Set the compression type. Default: "Deflate".
     *
     * @param compression One of "Deflate", "LZW", "JPEG", "PackBits"
     * @return this builder
     */
    public Builder compression(String compression) {
      this.compression = compression;
      return this;
    }

    /**
     * Set the compression quality. Default: 0.2.
     *
     * @param compressionQuality Value from 0.0 (max compression) to 1.0 (no compression)
     * @return this builder
     */
    public Builder compressionQuality(double compressionQuality) {
      this.compressionQuality = compressionQuality;
      return this;
    }

    /**
     * Set the tile size for both width and height. Default: 256.
     *
     * @param tileSize Must be a positive power of 2 (e.g. 128, 256, 512, 1024)
     * @return this builder
     */
    public Builder tileSize(int tileSize) {
      this.tileSize = tileSize;
      return this;
    }

    /**
     * Set the resampling algorithm for overview generation. Default: "Nearest".
     *
     * @param resampling One of "Nearest", "Bilinear", "Bicubic"
     * @return this builder
     */
    public Builder resampling(String resampling) {
      this.resampling = resampling;
      return this;
    }

    /**
     * Set the number of overview levels. Default: -1 (auto-compute).
     *
     * @param overviewCount -1 for auto, 0 for no overviews, or a positive count
     * @return this builder
     */
    public Builder overviewCount(int overviewCount) {
      this.overviewCount = overviewCount;
      return this;
    }

    /**
     * Build and validate the options.
     *
     * @return A validated, immutable {@link CogOptions} instance
     * @throws IllegalArgumentException if any option is invalid
     */
    public CogOptions build() {
      if (compression == null || compression.isEmpty()) {
        throw new IllegalArgumentException("compression must not be null or empty");
      }
      if (!VALID_COMPRESSION.contains(compression)) {
        throw new IllegalArgumentException(
            "compression must be one of " + VALID_COMPRESSION + ", got: '" + compression + "'");
      }
      if (compressionQuality < 0 || compressionQuality > 1.0) {
        throw new IllegalArgumentException(
            "compressionQuality must be between 0.0 and 1.0, got: " + compressionQuality);
      }
      if (tileSize <= 0) {
        throw new IllegalArgumentException("tileSize must be positive, got: " + tileSize);
      }
      if ((tileSize & (tileSize - 1)) != 0) {
        throw new IllegalArgumentException("tileSize must be a power of 2, got: " + tileSize);
      }
      if (overviewCount < -1) {
        throw new IllegalArgumentException(
            "overviewCount must be -1 (auto), 0 (none), or positive, got: " + overviewCount);
      }

      // Normalize resampling to title-case for matching
      String normalized = normalizeResampling(resampling);
      if (!VALID_RESAMPLING.contains(normalized)) {
        throw new IllegalArgumentException(
            "resampling must be one of " + VALID_RESAMPLING + ", got: '" + resampling + "'");
      }
      this.resampling = normalized;

      return new CogOptions(this);
    }

    /**
     * Normalize the resampling string to title-case (first letter uppercase, rest lowercase) so
     * callers can pass "nearest", "BILINEAR", etc.
     */
    private static String normalizeResampling(String value) {
      if (value == null || value.isEmpty()) {
        return "Nearest";
      }
      String lower = value.toLowerCase(Locale.ROOT);
      return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }
  }
}
