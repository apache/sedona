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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HadoopImageInputStreamTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final int TEST_FILE_SIZE = 1000;
  private final Random random = new Random();
  private File testFile;

  @Before
  public void setup() throws IOException {
    testFile = temp.newFile();
    prepareTestData(testFile);
  }

  @Test
  public void testReadSequentially() throws IOException {
    Path path = new Path(testFile.getPath());
    try (HadoopImageInputStream stream = new HadoopImageInputStream(path);
        InputStream in = new BufferedInputStream(Files.newInputStream(testFile.toPath()))) {
      byte[] bActual = new byte[8];
      byte[] bExpected = new byte[bActual.length];
      while (true) {
        int len = random.nextInt(bActual.length + 1);
        int lenActual = stream.read(bActual, 0, len);
        int lenExpected = in.read(bExpected, 0, len);
        Assert.assertEquals(lenExpected, lenActual);
        if (lenActual < 0) {
          break;
        }
        Assert.assertArrayEquals(bExpected, bActual);
      }
    }
  }

  @Test
  public void testReadRandomly() throws IOException {
    Path path = new Path(testFile.getPath());
    try (HadoopImageInputStream stream = new HadoopImageInputStream(path);
        RandomAccessFile raf = new RandomAccessFile(testFile, "r")) {
      byte[] bActual = new byte[8];
      byte[] bExpected = new byte[bActual.length];
      for (int k = 0; k < 1000; k++) {
        int offset = random.nextInt(TEST_FILE_SIZE + 1);
        int len = random.nextInt(bActual.length + 1);
        stream.seek(offset);
        raf.seek(offset);
        int lenActual = stream.read(bActual, 0, len);
        int lenExpected = raf.read(bExpected, 0, len);
        Assert.assertEquals(lenExpected, lenActual);
        if (lenActual < 0) {
          continue;
        }
        Assert.assertArrayEquals(bExpected, bActual);
      }

      // Test seek to EOF.
      stream.seek(TEST_FILE_SIZE);
      int len = stream.read(bActual, 0, bActual.length);
      Assert.assertEquals(-1, len);
    }
  }

  @Test
  public void testFromUnstableStream() throws IOException {
    Path path = new Path(testFile.getPath());
    FileSystem fs = path.getFileSystem(new Configuration());
    try (FSDataInputStream unstable = new UnstableFSDataInputStream(fs.open(path));
        HadoopImageInputStream stream = new HadoopImageInputStream(unstable);
        InputStream in = new BufferedInputStream(Files.newInputStream(testFile.toPath()))) {
      byte[] bActual = new byte[8];
      byte[] bExpected = new byte[bActual.length];
      while (true) {
        int len = random.nextInt(bActual.length);
        int lenActual = stream.read(bActual, 0, len);
        int lenExpected = in.read(bExpected, 0, len);
        Assert.assertEquals(lenExpected, lenActual);
        if (lenActual < 0) {
          break;
        }
        Assert.assertArrayEquals(bExpected, bActual);
      }
    }
  }

  private void prepareTestData(File testFile) throws IOException {
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(testFile.toPath()))) {
      for (int k = 0; k < TEST_FILE_SIZE; k++) {
        out.write(random.nextInt());
      }
    }
  }

  /**
   * An FSDataInputStream that sometimes return less data than requested when calling read(byte[],
   * int, int).
   */
  private static class UnstableFSDataInputStream extends FSDataInputStream {
    public UnstableFSDataInputStream(FSDataInputStream in) {
      super(new UnstableInputStream(in.getWrappedStream()));
    }

    private static class UnstableInputStream extends InputStream
        implements Seekable, PositionedReadable {
      private final InputStream wrapped;
      private final Random random = new Random();

      UnstableInputStream(InputStream in) {
        wrapped = in;
      }

      @Override
      public void close() throws IOException {
        wrapped.close();
      }

      @Override
      public int read() throws IOException {
        return wrapped.read();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        // Make this read unstable, i.e. sometimes return less data than requested,
        // but still obey InputStream's contract by not returning 0 when len > 0.
        if (len == 0) {
          return 0;
        }
        int unstableLen = 1 + random.nextInt(len);
        return wrapped.read(b, off, unstableLen);
      }

      @Override
      public void seek(long pos) throws IOException {
        ((Seekable) wrapped).seek(pos);
      }

      @Override
      public long getPos() throws IOException {
        return ((Seekable) wrapped).getPos();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return ((Seekable) wrapped).seekToNewSource(targetPos);
      }

      @Override
      public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return ((PositionedReadable) wrapped).read(position, buffer, offset, length);
      }

      @Override
      public void readFully(long position, byte[] buffer, int offset, int length)
          throws IOException {
        ((PositionedReadable) wrapped).readFully(position, buffer, offset, length);
      }

      @Override
      public void readFully(long position, byte[] buffer) throws IOException {
        ((PositionedReadable) wrapped).readFully(position, buffer);
      }
    }
  }
}
