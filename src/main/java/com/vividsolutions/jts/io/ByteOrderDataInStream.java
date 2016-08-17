/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.vividsolutions.jts.io;

import java.io.IOException;

/**
 * Allows reading a stream of Java primitive datatypes from an underlying
 * {@link InStream},
 * with the representation being in either common byte ordering.
 */
public class ByteOrderDataInStream
{
  private int byteOrder = ByteOrderValues.BIG_ENDIAN;
  private InStream stream;
  // buffers to hold primitive datatypes
  private byte[] buf1 = new byte[1];
  private byte[] buf4 = new byte[4];
  private byte[] buf8 = new byte[8];

  public ByteOrderDataInStream()
  {
    this.stream = null;
  }

  public ByteOrderDataInStream(InStream stream)
  {
    this.stream = stream;
  }

  /**
   * Allows a single ByteOrderDataInStream to be reused
   * on multiple InStreams.
   *
   * @param stream
   */
  public void setInStream(InStream stream)
  {
    this.stream = stream;
  }
  public void setOrder(int byteOrder)
  {
    this.byteOrder = byteOrder;
  }

  /**
   * Reads a byte value
   *
   * @return the byte read
   */
  public byte readByte()
  	throws IOException
  {
    stream.read(buf1);
    return buf1[0];
  }

  public int readInt()
	throws IOException
  {
    stream.read(buf4);
    return ByteOrderValues.getInt(buf4, byteOrder);
  }
  public long readLong()
	throws IOException
  {
    stream.read(buf8);
    return ByteOrderValues.getLong(buf8, byteOrder);
  }

  public double readDouble()
	throws IOException
  {
    stream.read(buf8);
    return ByteOrderValues.getDouble(buf8, byteOrder);
  }

}
