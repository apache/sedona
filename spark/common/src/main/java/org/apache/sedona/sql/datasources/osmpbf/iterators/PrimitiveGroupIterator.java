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
package org.apache.sedona.sql.datasources.osmpbf.iterators;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;
import org.apache.sedona.sql.datasources.osmpbf.TruncatedStream;
import org.apache.sedona.sql.datasources.osmpbf.build.Fileformat;

public class PrimitiveGroupIterator implements Iterator<BlobData> {
  TruncatedStream pbfInputStream;
  DataInputStream pbfStream;

  int nextHeaderSize;

  public PrimitiveGroupIterator(TruncatedStream pbfInputStream) {
    this.pbfInputStream = pbfInputStream;
    this.pbfStream = new DataInputStream(pbfInputStream);
    this.nextHeaderSize = -1;
    readNextBlockLength();
  }

  @Override
  public boolean hasNext() {
    return pbfInputStream.continueReading() && nextHeaderSize != -1;
  }

  @Override
  public BlobData next() {
    try {
      byte[] bufferBlobHeader = new byte[nextHeaderSize];
      pbfStream.readFully(bufferBlobHeader);
      Fileformat.BlobHeader blobHeader = Fileformat.BlobHeader.parseFrom(bufferBlobHeader);

      byte[] bufferBlob = new byte[blobHeader.getDatasize()];
      pbfStream.readFully(bufferBlob);
      Fileformat.Blob blob = Fileformat.Blob.parseFrom(bufferBlob);
      readNextBlockLength();
      return new BlobData(blobHeader, blob);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void readNextBlockLength() {
    try {
      nextHeaderSize = pbfStream.readInt();
    } catch (EOFException e) {
      nextHeaderSize = -1;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
