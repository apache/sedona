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

/**
 * Allows an array of bytes to be used as an {@link InStream}.
 * To optimize memory usage, instances can be reused
 * with different byte arrays.
 */
public class ByteArrayInStream
	implements InStream
{
	/*
	 * Implementation improvement suggested by Andrea Aime - Dec 15 2007
	 */
	
  private byte[] buffer;
	private int position;

	/**
	 * Creates a new stream based on the given buffer.
	 * 
	 * @param buffer the bytes to read
	 */
	public ByteArrayInStream(final byte[] buffer) {
		setBytes(buffer);
	}

	/**
	 * Sets this stream to read from the given buffer
	 * 
	 * @param buffer the bytes to read
	 */
	public void setBytes(final byte[] buffer) {
		this.buffer = buffer;
		this.position = 0;
	}

	/**
	 * Reads up to <tt>buf.length</tt> bytes from the stream
	 * into the given byte buffer.
	 * 
	 * @param buf the buffer to place the read bytes into
	 */
	public void read(final byte[] buf) {
		int numToRead = buf.length;
		// don't try and copy past the end of the input
		if ((position + numToRead) > buffer.length) {
			numToRead = buffer.length - position;
			System.arraycopy(buffer, position, buf, 0, numToRead);
			// zero out the unread bytes
			for (int i = numToRead; i < buf.length; i++) {
				buf[i] = 0;
			}
		}
		else {
			System.arraycopy(buffer, position, buf, 0, numToRead);			
		}
		position += numToRead;
	}
}
