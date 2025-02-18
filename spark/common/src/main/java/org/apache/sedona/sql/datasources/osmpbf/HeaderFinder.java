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
import java.util.LinkedList;

public class HeaderFinder {
  private byte[] pattern = new byte[] {0x0A, 0x07, 'O', 'S', 'M', 'D', 'a', 't', 'a'};
  private int HEADER_SIZE_LENGTH = 4;

  int idx = 0;

  InputStream stream;

  public HeaderFinder(InputStream stream) {
    this.stream = stream;
  }

  public long find() throws IOException {
    stream.skip(HEADER_SIZE_LENGTH);

    LinkedList<Byte> current = new LinkedList<>();

    while (current.size() < pattern.length) {
      int i = stream.read();
      if (i == -1) {
        return -1;
      }

      current.add((byte) i);
    }

    int i = 0;

    while (!equals(current, pattern) && i != -1) {
      i = stream.read();
      if (i == -1) {
        return -1;
      }

      current.removeFirst();
      current.add((byte) i);

      idx += 1;
    }

    if (equals(current, pattern)) {
      return idx;
    }

    return 0;
  }

  public boolean equals(LinkedList<Byte> a, byte[] b) {
    for (int i = 0; i < a.size(); i++) {
      if (a.get(i) != b[i]) {
        return false;
      }
    }

    return true;
  }
}
