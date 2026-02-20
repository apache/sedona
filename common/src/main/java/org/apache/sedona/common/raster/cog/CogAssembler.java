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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Assembles multiple parsed TIFF IFDs into Cloud Optimized GeoTIFF (COG) byte order.
 *
 * <p>COG layout (per the spec):
 *
 * <pre>
 *   [TIFF header - 8 bytes]
 *   [IFD 0: full-res tags + overflow data]
 *   [IFD 1: overview 2x tags + overflow data]
 *   ...
 *   [IFD N: smallest overview tags + overflow data]
 *   [smallest overview image data]
 *   ...
 *   [overview 2x image data]
 *   [full-res image data]
 * </pre>
 *
 * <p>Key COG requirements:
 *
 * <ul>
 *   <li>All IFDs are contiguous at the start of the file
 *   <li>Image data follows all IFDs, ordered smallest overview first, full-res last
 *   <li>TileOffsets/StripOffsets point forward to where image data will be located
 *   <li>Overviews have NewSubfileType = 1 (ReducedImage)
 * </ul>
 *
 * <p>Ported from GeoTrellis's {@code GeoTiffWriter.appendCloudOptimized()}.
 */
public class CogAssembler {

  /** NewSubfileType value for reduced-resolution (overview) images */
  private static final int REDUCED_IMAGE = 1;

  /** Result of patching IFD entries: contains both the patched entries and overflow data. */
  private static class PatchedIfd {
    final byte[] entries;
    final byte[] overflow;

    PatchedIfd(byte[] entries, byte[] overflow) {
      this.entries = entries;
      this.overflow = overflow;
    }
  }

  /**
   * Assemble parsed TIFF IFDs into COG byte order, returning a byte array.
   *
   * @param parsedTiffs List of parsed TIFFs, ordered: [full-res, overview-2x, overview-4x, ...
   *     smallest]. The first element is the full resolution image, subsequent elements are
   *     progressively smaller overviews.
   * @return A byte array containing the complete COG file
   * @throws IOException if writing fails
   */
  public static byte[] assemble(List<TiffIfdParser.ParsedTiff> parsedTiffs) throws IOException {
    // Compute total size for pre-allocated buffer
    long totalSize = computeTotalSize(parsedTiffs);
    ByteArrayOutputStream bos =
        new ByteArrayOutputStream((int) Math.min(totalSize, Integer.MAX_VALUE));
    assemble(parsedTiffs, bos);
    return bos.toByteArray();
  }

  /**
   * Assemble parsed TIFF IFDs into COG byte order, writing directly to the given output stream.
   * This avoids allocating a final byte[] for the entire COG, making it suitable for writing large
   * COGs to disk or network streams.
   *
   * @param parsedTiffs List of parsed TIFFs, ordered: [full-res, overview-2x, overview-4x, ...
   *     smallest].
   * @param outputStream The stream to write the COG to. Not closed by this method.
   * @throws IOException if writing fails
   */
  public static void assemble(List<TiffIfdParser.ParsedTiff> parsedTiffs, OutputStream outputStream)
      throws IOException {
    if (parsedTiffs.isEmpty()) {
      throw new IllegalArgumentException("No TIFFs to assemble");
    }

    ByteOrder byteOrder = parsedTiffs.get(0).byteOrder;
    int ifdCount = parsedTiffs.size();

    // Determine which overview IFDs need NewSubfileType injection
    boolean[] needsNewSubfileType = new boolean[ifdCount];
    for (int i = 1; i < ifdCount; i++) {
      needsNewSubfileType[i] = !parsedTiffs.get(i).hasNewSubfileType;
    }

    // Phase 1: Compute sizes of all IFD regions (IFD entries + overflow data)
    // If we need to inject NewSubfileType, the IFD grows by 12 bytes (one tag entry)
    int[] ifdRegionSizes = new int[ifdCount];
    int[] effectiveTagCounts = new int[ifdCount];
    for (int i = 0; i < ifdCount; i++) {
      TiffIfdParser.ParsedTiff pt = parsedTiffs.get(i);
      int extraBytes = needsNewSubfileType[i] ? 12 : 0;
      effectiveTagCounts[i] = pt.tagCount + (needsNewSubfileType[i] ? 1 : 0);
      ifdRegionSizes[i] = pt.getIfdAndOverflowSize() + extraBytes;
    }

    // Phase 2: Compute absolute offsets for each IFD and its image data.
    // Layout: [header=8] [IFD0+overflow] [IFD1+overflow] ... [IFDN+overflow]
    //         [imageN] ... [image1] [image0]
    int[] ifdAbsoluteOffsets = new int[ifdCount];
    int cursor = 8; // After TIFF header
    for (int i = 0; i < ifdCount; i++) {
      ifdAbsoluteOffsets[i] = cursor;
      cursor += ifdRegionSizes[i];
    }
    int imageDataRegionStart = cursor;

    // Image data is written in reverse order (smallest overview first, full-res last)
    // Compute absolute offset of each IFD's image data
    int[] imageDataAbsoluteOffsets = new int[ifdCount];
    int imageDataCursor = imageDataRegionStart;
    for (int i = ifdCount - 1; i >= 0; i--) {
      imageDataAbsoluteOffsets[i] = imageDataCursor;
      imageDataCursor += parsedTiffs.get(i).imageDataLength;
    }

    // Phase 3: Write the COG
    DataOutputStream dos = new DataOutputStream(outputStream);

    // Write TIFF header
    if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
      dos.writeByte('I');
      dos.writeByte('I');
    } else {
      dos.writeByte('M');
      dos.writeByte('M');
    }
    writeShort(dos, byteOrder, 42); // TIFF magic
    writeInt(dos, byteOrder, ifdAbsoluteOffsets[0]); // Offset to first IFD

    // Write each IFD + its overflow data
    for (int i = 0; i < ifdCount; i++) {
      TiffIfdParser.ParsedTiff pt = parsedTiffs.get(i);
      boolean isOverview = i > 0;
      int ifdStart = ifdAbsoluteOffsets[i];
      int nextIfdOffset = (i + 1 < ifdCount) ? ifdAbsoluteOffsets[i + 1] : 0;
      int tagCountForIfd = effectiveTagCounts[i];

      // Compute where this IFD's overflow data will be in the output
      // Account for possible extra 12 bytes from injected tag
      int overflowStartInOutput = ifdStart + 2 + tagCountForIfd * 12 + 4;

      // Patch the IFD entries:
      // - Rebase overflow pointers from original file offsets to new output offsets
      // - Rewrite TileOffsets/StripOffsets to point to the new image data location
      // - Inject NewSubfileType=1 for overview IFDs if missing
      PatchedIfd patched =
          patchIfdEntries(
              pt,
              overflowStartInOutput,
              imageDataAbsoluteOffsets[i],
              isOverview,
              needsNewSubfileType[i],
              byteOrder);

      // Write: tag count (2 bytes) + entries (tagCountForIfd*12) + next IFD offset (4 bytes)
      writeShort(dos, byteOrder, tagCountForIfd);
      dos.write(patched.entries);
      writeInt(dos, byteOrder, nextIfdOffset);

      // Write patched overflow data
      dos.write(patched.overflow);
    }

    // Write image data in reverse order (smallest overview first)
    // Zero-copy: write directly from the source TIFF byte arrays
    for (int i = ifdCount - 1; i >= 0; i--) {
      TiffIfdParser.ParsedTiff pt = parsedTiffs.get(i);
      dos.write(pt.sourceData, pt.imageDataOffset, pt.imageDataLength);
    }

    dos.flush();
  }

  /**
   * Compute the total output size of the COG file. Used to pre-allocate the byte array in {@link
   * #assemble(List)}.
   */
  private static long computeTotalSize(List<TiffIfdParser.ParsedTiff> parsedTiffs) {
    long size = 8; // TIFF header
    for (int i = 0; i < parsedTiffs.size(); i++) {
      TiffIfdParser.ParsedTiff pt = parsedTiffs.get(i);
      boolean needsInject = i > 0 && !pt.hasNewSubfileType;
      int extraBytes = needsInject ? 12 : 0;
      size += pt.getIfdAndOverflowSize() + extraBytes;
      size += pt.imageDataLength;
    }
    return size;
  }

  /**
   * Patch IFD entries to update:
   *
   * <ol>
   *   <li>Overflow data pointers (rebase from original file offset to new output offset)
   *   <li>TileOffsets/StripOffsets values (point to new image data location)
   *   <li>Set or inject NewSubfileType=1 for overview IFDs
   * </ol>
   */
  private static PatchedIfd patchIfdEntries(
      TiffIfdParser.ParsedTiff pt,
      int newOverflowStart,
      int newImageDataStart,
      boolean isOverview,
      boolean injectNewSubfileType,
      ByteOrder byteOrder) {

    byte[] entries = pt.ifdEntries.clone();
    byte[] patchedOverflow = pt.overflowData.clone();
    ByteBuffer buf = ByteBuffer.wrap(entries).order(byteOrder);

    int overflowDelta = newOverflowStart - pt.overflowDataStart;

    for (int i = 0; i < pt.tagCount; i++) {
      int offset = i * 12;
      int tag = buf.getShort(offset) & 0xFFFF;
      int fieldType = buf.getShort(offset + 2) & 0xFFFF;
      int count = buf.getInt(offset + 4);
      int valueSize = count * getFieldTypeSize(fieldType);

      // Handle NewSubfileType tag for overviews (when already present)
      if (tag == TiffIfdParser.TAG_NEW_SUBFILE_TYPE && isOverview) {
        buf.putInt(offset + 8, REDUCED_IMAGE);
        continue;
      }

      // Handle TileOffsets/StripOffsets — rewrite to point to new image data location
      if (tag == TiffIfdParser.TAG_TILE_OFFSETS || tag == TiffIfdParser.TAG_STRIP_OFFSETS) {
        if (count == 1 && valueSize <= 4) {
          // Single segment: offset stored inline
          buf.putInt(offset + 8, newImageDataStart + pt.segmentOffsets[0]);
        } else {
          // Multiple segments: the entry points to an overflow array.
          // We need to rewrite the overflow array with new absolute offsets.
          // First, rebase the pointer to the overflow data.
          int origPointer = buf.getInt(offset + 8);
          int newPointer = origPointer + overflowDelta;
          buf.putInt(offset + 8, newPointer);

          // Now patch the overflow data copy with new image data offsets
          int overflowArrayOffset = origPointer - pt.overflowDataStart;
          ByteBuffer overflowBuf = ByteBuffer.wrap(patchedOverflow).order(byteOrder);
          for (int j = 0; j < count; j++) {
            int newSegmentOffset = newImageDataStart + pt.segmentOffsets[j];
            overflowBuf.putInt(overflowArrayOffset + j * 4, newSegmentOffset);
          }
        }
        continue;
      }

      // For all other tags with overflow data (value > 4 bytes), rebase the pointer
      if (valueSize > 4) {
        int origPointer = buf.getInt(offset + 8);
        buf.putInt(offset + 8, origPointer + overflowDelta);
      }
    }

    // If we need to inject NewSubfileType, insert a 12-byte entry in sorted tag order
    if (injectNewSubfileType) {
      return new PatchedIfd(
          insertNewSubfileTypeTag(entries, pt.tagCount, byteOrder), patchedOverflow);
    }

    return new PatchedIfd(entries, patchedOverflow);
  }

  /**
   * Insert a NewSubfileType tag entry (tag 254) into a sorted IFD entry array. The new entry is
   * placed at the correct position to maintain tag sort order, and existing entries after it are
   * shifted right by 12 bytes.
   *
   * @param entries The original IFD entries (tagCount * 12 bytes)
   * @param tagCount The original number of tags
   * @param byteOrder The byte order for writing the new entry
   * @return A new byte array that is 12 bytes longer, with the NewSubfileType entry inserted
   */
  private static byte[] insertNewSubfileTypeTag(byte[] entries, int tagCount, ByteOrder byteOrder) {
    // NewSubfileType = tag 254, very low, typically goes near the start
    byte[] newEntries = new byte[entries.length + 12];
    ByteBuffer srcBuf = ByteBuffer.wrap(entries).order(byteOrder);

    // Find insertion point: first tag with code > 254
    int insertIdx = tagCount; // default: append at end
    for (int i = 0; i < tagCount; i++) {
      int tag = srcBuf.getShort(i * 12) & 0xFFFF;
      if (tag > TiffIfdParser.TAG_NEW_SUBFILE_TYPE) {
        insertIdx = i;
        break;
      }
    }

    // Copy entries before insertion point
    int beforeBytes = insertIdx * 12;
    if (beforeBytes > 0) {
      System.arraycopy(entries, 0, newEntries, 0, beforeBytes);
    }

    // Write the NewSubfileType entry at insertion point
    ByteBuffer newBuf = ByteBuffer.wrap(newEntries).order(byteOrder);
    int insertOffset = insertIdx * 12;
    newBuf.putShort(insertOffset, (short) TiffIfdParser.TAG_NEW_SUBFILE_TYPE); // tag = 254
    newBuf.putShort(insertOffset + 2, (short) 4); // type = LONG
    newBuf.putInt(insertOffset + 4, 1); // count = 1
    newBuf.putInt(insertOffset + 8, REDUCED_IMAGE); // value = 1

    // Copy entries after insertion point
    int afterBytes = entries.length - beforeBytes;
    if (afterBytes > 0) {
      System.arraycopy(entries, beforeBytes, newEntries, beforeBytes + 12, afterBytes);
    }

    return newEntries;
  }

  /** Write a 16-bit value respecting byte order */
  private static void writeShort(DataOutputStream dos, ByteOrder order, int value)
      throws IOException {
    if (order == ByteOrder.LITTLE_ENDIAN) {
      dos.writeByte(value & 0xFF);
      dos.writeByte((value >>> 8) & 0xFF);
    } else {
      dos.writeByte((value >>> 8) & 0xFF);
      dos.writeByte(value & 0xFF);
    }
  }

  /** Write a 32-bit value respecting byte order */
  private static void writeInt(DataOutputStream dos, ByteOrder order, int value)
      throws IOException {
    if (order == ByteOrder.LITTLE_ENDIAN) {
      dos.writeByte(value & 0xFF);
      dos.writeByte((value >>> 8) & 0xFF);
      dos.writeByte((value >>> 16) & 0xFF);
      dos.writeByte((value >>> 24) & 0xFF);
    } else {
      dos.writeByte((value >>> 24) & 0xFF);
      dos.writeByte((value >>> 16) & 0xFF);
      dos.writeByte((value >>> 8) & 0xFF);
      dos.writeByte(value & 0xFF);
    }
  }

  /** Get the byte size of a TIFF field type */
  private static int getFieldTypeSize(int fieldType) {
    int[] sizes = {0, 1, 1, 2, 4, 8, 1, 1, 2, 4, 8, 4, 8};
    if (fieldType >= 1 && fieldType < sizes.length) {
      return sizes[fieldType];
    }
    return 1;
  }
}
