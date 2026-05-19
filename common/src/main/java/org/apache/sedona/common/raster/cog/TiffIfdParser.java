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
package org.apache.sedona.common.raster.cog;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Parses the IFD (Image File Directory) structure from a TIFF byte array. This is used to extract
 * the structural components needed for COG assembly: the IFD entries, overflow tag data, and image
 * data regions.
 *
 * <p>Reference: TIFF 6.0 Specification, Section 2 (TIFF Structure).
 */
public class TiffIfdParser {

  /** Tag code for TileOffsets (0x0144 = 324) */
  public static final int TAG_TILE_OFFSETS = 324;

  /** Tag code for StripOffsets (0x0111 = 273) */
  public static final int TAG_STRIP_OFFSETS = 273;

  /** Tag code for TileByteCounts (0x0145 = 325) */
  public static final int TAG_TILE_BYTE_COUNTS = 325;

  /** Tag code for StripByteCounts (0x0117 = 279) */
  public static final int TAG_STRIP_BYTE_COUNTS = 279;

  /** Tag code for NewSubfileType (0x00FE = 254) */
  public static final int TAG_NEW_SUBFILE_TYPE = 254;

  /** TIFF field type sizes in bytes */
  private static final int[] FIELD_TYPE_SIZES = {
    0, // 0: unused
    1, // 1: BYTE
    1, // 2: ASCII
    2, // 3: SHORT
    4, // 4: LONG
    8, // 5: RATIONAL
    1, // 6: SBYTE
    1, // 7: UNDEFINED
    2, // 8: SSHORT
    4, // 9: SLONG
    8, // 10: SRATIONAL
    4, // 11: FLOAT
    8 // 12: DOUBLE
  };

  /**
   * Result of parsing a TIFF file. Contains the byte order and the parsed IFD data for the first
   * IFD only (we write each overview as a separate TIFF, so there's always exactly one IFD).
   */
  public static class ParsedTiff {
    /** Byte order of the TIFF file */
    public final ByteOrder byteOrder;

    /** Offset where the first IFD starts (always 8 for standard TIFF) */
    public final int ifdOffset;

    /** Number of tag entries in the IFD */
    public final int tagCount;

    /**
     * Raw bytes of all IFD tag entries (tagCount * 12 bytes). This includes the 12-byte entries but
     * NOT the 2-byte tag count or the 4-byte next-IFD pointer.
     */
    public final byte[] ifdEntries;

    /**
     * Overflow tag data — values that exceed 4 bytes and are stored outside the IFD entries. This
     * is collected in the order the tags reference them.
     */
    public final byte[] overflowData;

    /**
     * The start offset of the overflow data region in the original TIFF file. Used to rebase
     * overflow pointers when reassembling.
     */
    public final int overflowDataStart;

    /**
     * The original source TIFF byte array. Image data is referenced via {@link #imageDataOffset}
     * and {@link #imageDataLength} rather than being copied out, to avoid a redundant allocation.
     */
    public final byte[] sourceData;

    /** Byte offset within {@link #sourceData} where image data begins */
    public final int imageDataOffset;

    /** Total byte length of the image data region */
    public final int imageDataLength;

    /** Byte offsets of each tile/strip within imageData (relative to imageData start) */
    public final int[] segmentOffsets;

    /** Byte counts of each tile/strip */
    public final int[] segmentByteCounts;

    /** Whether the IFD contains a NewSubfileType tag */
    public final boolean hasNewSubfileType;

    /** The total size of the IFD region: 2 (count) + tagCount*12 + 4 (next pointer) */
    public int getIfdSize() {
      return 2 + tagCount * 12 + 4;
    }

    /** The total size of IFD + overflow data (everything before image data) */
    public int getIfdAndOverflowSize() {
      return getIfdSize() + overflowData.length;
    }

    ParsedTiff(
        ByteOrder byteOrder,
        int ifdOffset,
        int tagCount,
        byte[] ifdEntries,
        byte[] overflowData,
        int overflowDataStart,
        byte[] sourceData,
        int imageDataOffset,
        int imageDataLength,
        int[] segmentOffsets,
        int[] segmentByteCounts,
        boolean hasNewSubfileType) {
      this.byteOrder = byteOrder;
      this.ifdOffset = ifdOffset;
      this.tagCount = tagCount;
      this.ifdEntries = ifdEntries;
      this.overflowData = overflowData;
      this.overflowDataStart = overflowDataStart;
      this.sourceData = sourceData;
      this.imageDataOffset = imageDataOffset;
      this.imageDataLength = imageDataLength;
      this.segmentOffsets = segmentOffsets;
      this.segmentByteCounts = segmentByteCounts;
      this.hasNewSubfileType = hasNewSubfileType;
    }
  }

  /**
   * Parse a standard TIFF byte array and extract its first IFD structure.
   *
   * @param tiffBytes The complete TIFF file as a byte array
   * @return ParsedTiff with all structural components extracted
   * @throws IllegalArgumentException if the TIFF header is invalid
   */
  public static ParsedTiff parse(byte[] tiffBytes) {
    if (tiffBytes.length < 8) {
      throw new IllegalArgumentException("TIFF data too short: " + tiffBytes.length + " bytes");
    }

    // Read byte order from first 2 bytes
    ByteOrder byteOrder;
    if (tiffBytes[0] == 'I' && tiffBytes[1] == 'I') {
      byteOrder = ByteOrder.LITTLE_ENDIAN;
    } else if (tiffBytes[0] == 'M' && tiffBytes[1] == 'M') {
      byteOrder = ByteOrder.BIG_ENDIAN;
    } else {
      throw new IllegalArgumentException(
          "Invalid TIFF byte order marker: " + tiffBytes[0] + ", " + tiffBytes[1]);
    }

    ByteBuffer buf = ByteBuffer.wrap(tiffBytes).order(byteOrder);

    // Verify TIFF magic number (42)
    int magic = buf.getShort(2) & 0xFFFF;
    if (magic != 42) {
      throw new IllegalArgumentException("Not a standard TIFF file (magic=" + magic + ")");
    }

    // Read first IFD offset
    int ifdOffset = buf.getInt(4);
    if (ifdOffset < 8 || ifdOffset >= tiffBytes.length - 2) {
      throw new IllegalArgumentException(
          "IFD offset out of range: " + ifdOffset + " (file size: " + tiffBytes.length + ")");
    }

    // Read number of directory entries
    int tagCount = buf.getShort(ifdOffset) & 0xFFFF;

    // Read all IFD entries (12 bytes each)
    int entriesStart = ifdOffset + 2;
    int entriesLen = tagCount * 12;
    if (entriesStart + entriesLen > tiffBytes.length) {
      throw new IllegalArgumentException(
          "IFD entries extend beyond file: entriesStart="
              + entriesStart
              + " entriesLen="
              + entriesLen
              + " fileSize="
              + tiffBytes.length);
    }
    byte[] ifdEntries = new byte[entriesLen];
    System.arraycopy(tiffBytes, entriesStart, ifdEntries, 0, entriesLen);

    // Find the offsets tag and bytecounts tag to locate image data
    int offsetsTag = -1;
    int byteCountsTag = -1;
    int segmentCount = 0;
    boolean hasNewSubfileType = false;

    // Also track the overflow data region
    int overflowStart = Integer.MAX_VALUE;
    int overflowEnd = 0;

    // First pass: find offset/bytecount tags and overflow region
    for (int i = 0; i < tagCount; i++) {
      int entryOffset = entriesStart + i * 12;
      int tag = buf.getShort(entryOffset) & 0xFFFF;
      int fieldType = buf.getShort(entryOffset + 2) & 0xFFFF;
      int count = buf.getInt(entryOffset + 4);
      int valueSize = count * getFieldTypeSize(fieldType);

      if (tag == TAG_TILE_OFFSETS || tag == TAG_STRIP_OFFSETS) {
        offsetsTag = tag;
        segmentCount = count;
      } else if (tag == TAG_TILE_BYTE_COUNTS || tag == TAG_STRIP_BYTE_COUNTS) {
        byteCountsTag = tag;
      } else if (tag == TAG_NEW_SUBFILE_TYPE) {
        hasNewSubfileType = true;
      }

      // Track overflow data region (values > 4 bytes stored outside IFD entries)
      if (valueSize > 4) {
        int valOffset = buf.getInt(entryOffset + 8);
        if (valOffset < 0 || valOffset + valueSize > tiffBytes.length) {
          throw new IllegalArgumentException(
              "Overflow data for tag "
                  + tag
                  + " out of range: offset="
                  + valOffset
                  + " size="
                  + valueSize
                  + " fileSize="
                  + tiffBytes.length);
        }
        overflowStart = Math.min(overflowStart, valOffset);
        overflowEnd = Math.max(overflowEnd, valOffset + valueSize);
      }
    }

    if (offsetsTag < 0 || byteCountsTag < 0) {
      throw new IllegalArgumentException(
          "TIFF missing TileOffsets/StripOffsets or TileByteCounts/StripByteCounts tags");
    }

    // Read segment offsets and byte counts
    int[] segmentOffsets = readIntArray(buf, tiffBytes, entriesStart, tagCount, offsetsTag);
    int[] segmentByteCounts = readIntArray(buf, tiffBytes, entriesStart, tagCount, byteCountsTag);

    // Extract overflow data
    byte[] overflowData;
    int overflowDataStart;
    if (overflowStart < Integer.MAX_VALUE) {
      overflowDataStart = overflowStart;
      overflowData = new byte[overflowEnd - overflowStart];
      System.arraycopy(tiffBytes, overflowStart, overflowData, 0, overflowData.length);
    } else {
      overflowDataStart = 0;
      overflowData = new byte[0];
    }

    // Find image data bounds
    int imageDataStart = Integer.MAX_VALUE;
    int imageDataEnd = 0;
    for (int i = 0; i < segmentCount; i++) {
      imageDataStart = Math.min(imageDataStart, segmentOffsets[i]);
      imageDataEnd = Math.max(imageDataEnd, segmentOffsets[i] + segmentByteCounts[i]);
    }

    if (imageDataStart < 0 || imageDataEnd > tiffBytes.length) {
      throw new IllegalArgumentException(
          "Image data region out of range: start="
              + imageDataStart
              + " end="
              + imageDataEnd
              + " fileSize="
              + tiffBytes.length);
    }

    int imageDataLength = imageDataEnd - imageDataStart;

    // Make segment offsets relative to imageData start
    int[] relativeOffsets = new int[segmentCount];
    for (int i = 0; i < segmentCount; i++) {
      relativeOffsets[i] = segmentOffsets[i] - imageDataStart;
    }

    return new ParsedTiff(
        byteOrder,
        ifdOffset,
        tagCount,
        ifdEntries,
        overflowData,
        overflowDataStart,
        tiffBytes,
        imageDataStart,
        imageDataLength,
        relativeOffsets,
        segmentByteCounts,
        hasNewSubfileType);
  }

  /**
   * Read an array of int values from an IFD tag entry. Handles both inline (count=1, value in
   * entry) and overflow (count>1, pointer in entry) cases.
   */
  private static int[] readIntArray(
      ByteBuffer buf, byte[] tiffBytes, int entriesStart, int tagCount, int targetTag) {
    for (int i = 0; i < tagCount; i++) {
      int entryOffset = entriesStart + i * 12;
      int tag = buf.getShort(entryOffset) & 0xFFFF;
      if (tag != targetTag) continue;

      int fieldType = buf.getShort(entryOffset + 2) & 0xFFFF;
      int count = buf.getInt(entryOffset + 4);

      int valueSize = count * getFieldTypeSize(fieldType);
      int[] result = new int[count];

      if (valueSize <= 4) {
        // Value stored inline in the entry
        if (fieldType == 3) { // SHORT
          for (int j = 0; j < count; j++) {
            result[j] = buf.getShort(entryOffset + 8 + j * 2) & 0xFFFF;
          }
        } else { // LONG
          result[0] = buf.getInt(entryOffset + 8);
        }
      } else {
        // Value stored at offset
        int valOffset = buf.getInt(entryOffset + 8);
        if (fieldType == 3) { // SHORT
          for (int j = 0; j < count; j++) {
            result[j] = buf.getShort(valOffset + j * 2) & 0xFFFF;
          }
        } else { // LONG
          for (int j = 0; j < count; j++) {
            result[j] = buf.getInt(valOffset + j * 4);
          }
        }
      }
      return result;
    }
    throw new IllegalArgumentException("Tag " + targetTag + " not found in IFD");
  }

  /** Get the byte size of a TIFF field type. */
  private static int getFieldTypeSize(int fieldType) {
    if (fieldType >= 1 && fieldType < FIELD_TYPE_SIZES.length) {
      return FIELD_TYPE_SIZES[fieldType];
    }
    return 1; // default for unknown types
  }
}
