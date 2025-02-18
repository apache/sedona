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
package org.apache.sedona.sql.datasources.osmpbf;

import java.io.IOException;
import java.io.InputStream;

public class StartEndStream extends TruncatedStream {
  private long counter = 0L;
  private long lengthLimit = 0L;

  @Override
  public boolean continueReading() {
    return lengthLimit > counter;
  }

  @Override
  public int read() throws IOException {
    int result = super.read();
    if (result != -1) counter += 1;
    return result;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int result = super.read(b, off, len);
    if (result != -1) counter += result;
    return result;
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = super.skip(n);
    counter += skipped;
    return skipped;
  }

  public StartEndStream(InputStream in, long lengthLimit) {
    super(in);
    this.lengthLimit = lengthLimit;
  }
}
