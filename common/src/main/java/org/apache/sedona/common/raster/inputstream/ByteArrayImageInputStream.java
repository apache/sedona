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
package org.apache.sedona.common.raster.inputstream;

import java.io.IOException;
import javax.imageio.stream.ImageInputStreamImpl;

public class ByteArrayImageInputStream extends ImageInputStreamImpl {

  private final byte[] bytes;

  public ByteArrayImageInputStream(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public int read() throws IOException {
    checkClosed();
    bitOffset = 0;
    if (streamPos >= bytes.length) {
      return -1;
    }
    byte b = bytes[(int) streamPos++];
    return b & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkClosed();

    if (b == null) {
      throw new NullPointerException("b == null!");
    }
    if (off < 0 || len < 0 || off + len > b.length || off + len < 0) {
      throw new IndexOutOfBoundsException(
          "off < 0 || len < 0 || off+len > b.length || off+len < 0!");
    }

    bitOffset = 0;

    if (len == 0) {
      return 0;
    }

    int remaining = (int) (bytes.length - streamPos);
    len = Math.min(len, remaining);
    if (len <= 0) {
      return -1;
    }

    System.arraycopy(bytes, (int) streamPos, b, off, len);
    streamPos += len;
    return len;
  }

  @Override
  public boolean isCached() {
    return true;
  }

  @Override
  public boolean isCachedMemory() {
    return true;
  }
}
