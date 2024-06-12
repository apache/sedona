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
package org.apache.sedona.core.formatMapper.netcdfParser;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ucar.unidata.io.RandomAccessFile;

public class HDFSRandomAccessFile extends RandomAccessFile {

  protected URI fsURI;
  protected Path filePath;
  protected FSDataInputStream hfile;
  protected FileStatus fileStatus;

  public HDFSRandomAccessFile(String fileSystemURI, String location) throws IOException {
    this(fileSystemURI, location, RandomAccessFile.defaultBufferSize);
  }

  public HDFSRandomAccessFile(String fileSystemURI, String location, int bufferSize)
      throws IOException {
    super(bufferSize);
    fsURI = URI.create(fileSystemURI);
    filePath = new Path(location);
    this.location = location;
    if (RandomAccessFile.debugLeaks) {
      RandomAccessFile.openFiles.add(location);
    }

    FileSystem fs = FileSystem.get(fsURI, new Configuration());
    hfile = fs.open(filePath);

    fileStatus = fs.getFileStatus(filePath);
  }

  @Override
  public void flush() {}

  @Override
  public synchronized void close() throws IOException {
    super.close();
    hfile.close();
  }

  public long getLastModified() {
    return fileStatus.getModificationTime();
  }

  @Override
  public long length() throws IOException {
    return fileStatus.getLen();
  }

  @Override
  protected int read_(long pos, byte[] b, int offset, int len) throws IOException {
    int n = hfile.read(pos, b, offset, len);
    return n;
  }

  @Override
  public long readToByteChannel(WritableByteChannel dest, long offset, long nbytes)
      throws IOException {
    long need = nbytes;
    byte[] buf = new byte[4096];

    hfile.seek(offset);
    int count = 0;
    while (need > 0 && count != -1) {
      need -= count;
      dest.write(ByteBuffer.wrap(buf, 0, count));
      count = hfile.read(buf, 0, 4096);
    }
    return nbytes - need;
  }
}
