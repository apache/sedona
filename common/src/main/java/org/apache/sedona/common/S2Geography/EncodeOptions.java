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

public class EncodeOptions {
  /** FAST writes raw doubles; COMPACT snaps vertices to cell centers. */
  public enum CodingHint {
    FAST,
    COMPACT
  }

  /** Default: FAST. */
  private CodingHint codingHint = CodingHint.FAST;

  /** If true, convert “hard” shapes into lazy‐decodable variants. */
  private boolean enableLazyDecode = false;

  /** If true, prefix the payload with the cell‐union covering. */
  private boolean includeCovering = false;

  public EncodeOptions() {}

  public EncodeOptions(EncodeOptions other) {
    this.codingHint = other.codingHint;
    this.enableLazyDecode = other.enableLazyDecode;
    this.includeCovering = other.includeCovering;
  }

  /** Control FAST vs. COMPACT encoding. */
  public void setCodingHint(CodingHint hint) {
    this.codingHint = hint;
  }

  public CodingHint getCodingHint() {
    return codingHint;
  }

  /** Enable or disable lazy‐decode conversions. */
  public void setEnableLazyDecode(boolean enable) {
    this.enableLazyDecode = enable;
  }

  public boolean isEnableLazyDecode() {
    return enableLazyDecode;
  }

  /** Include or omit the cell‐union covering prefix. */
  public void setIncludeCovering(boolean include) {
    this.includeCovering = include;
  }

  public boolean isIncludeCovering() {
    return includeCovering;
  }
}
