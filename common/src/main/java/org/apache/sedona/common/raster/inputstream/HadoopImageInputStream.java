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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** An ImageInputStream that reads image data from a Hadoop FileSystem. */
public class HadoopImageInputStream extends ImageInputStreamImpl {

  private final FSDataInputStream stream;
  private final Path path;
  private final Configuration conf;

  public HadoopImageInputStream(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    stream = fs.open(path);
    this.path = path;
    this.conf = conf;
  }

  public HadoopImageInputStream(Path path) throws IOException {
    this(path, new Configuration());
  }

  public HadoopImageInputStream(FSDataInputStream stream) {
    this.stream = stream;
    this.path = null;
    this.conf = null;
  }

  public Path getPath() {
    return path;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() throws IOException {
    super.close();
    stream.close();
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    int ret_len = read(buf, 0, 1);
    if (ret_len < 0) {
      return ret_len;
    }
    return buf[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkClosed();
    bitOffset = 0;

    if (len == 0) {
      return 0;
    }

    // stream.read may return fewer data than requested, so we need to loop until we get all the
    // data, or hit the end of the stream. We can not simply perform an incomplete read and return
    // the number of bytes actually read, since the methods in ImageInputStreamImpl such as
    // readInt() relies on this method and assumes that partial reads only happens when reading
    // EOF. This might be a bug of imageio since they should invoke readFully() in such cases.
    int remaining = len;
    while (remaining > 0) {
      int ret_len = stream.read(b, off, remaining);
      if (ret_len < 0) {
        // Hit EOF, no more data to read.
        if (len - remaining > 0) {
          // We have read some data, but that may not be all the data we can read from the
          // stream. The partial read may happen when reading from S3 (S3AInputStream), and it
          // may also happen on other remote file systems.
          return len - remaining;
        } else {
          // We have not read any data, return EOF.
          return ret_len;
        }
      }
      off += ret_len;
      remaining -= ret_len;
      streamPos += ret_len;
    }

    return len - remaining;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkClosed();
    stream.seek(pos);
    super.seek(pos);
  }
}
