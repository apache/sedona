# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import struct
import zlib
from io import BytesIO
from typing import Dict, List, Optional, Tuple, Union

import numpy as np

from .awt_raster import AWTRaster
from .data_buffer import DataBuffer
from .meta import AffineTransform, PixelAnchor, SampleDimension
from .sample_model import (
    ComponentSampleModel,
    MultiPixelPackedSampleModel,
    PixelInterleavedSampleModel,
    SampleModel,
    SinglePixelPackedSampleModel,
)
from .sedona_raster import InDbSedonaRaster, SedonaRaster


class RasterTypes:
    IN_DB = 0


def deserialize(buf: Union[bytearray, bytes]) -> Optional[SedonaRaster]:
    if buf is None:
        return None

    bio = BytesIO(buf)
    raster_type = int(bio.read(1)[0])
    return _deserialize(bio, raster_type)


def _deserialize(bio: BytesIO, raster_type: int) -> SedonaRaster:
    name = _read_utf8_string(bio)
    width, height, x, y = _read_grid_envelope(bio)
    affine_trans = _read_affine_transformation(bio)
    affine_trans = affine_trans.translate(x, y)
    affine_trans = affine_trans.with_anchor(PixelAnchor.UPPER_LEFT)
    crs_wkt = _read_crs_wkt(bio)
    bands_meta, category_blobs = _read_sample_dimensions(bio)
    if raster_type == RasterTypes.IN_DB:
        awt_raster, properties_blob, color_model_blob = _read_awt_raster(bio)
        raster = InDbSedonaRaster(
            width, height, bands_meta, affine_trans, crs_wkt, awt_raster
        )
        raster._name = name
        raster._category_blobs = category_blobs
        raster._properties_blob = properties_blob
        raster._color_model_blob = color_model_blob
        return raster
    else:
        raise ValueError(f"unsupported raster_type: {raster_type}")


def _read_grid_envelope(bio: BytesIO) -> Tuple[int, int, int, int]:
    width, height, x, y = struct.unpack("=iiii", bio.read(4 * 4))
    return (width, height, x, y)


def _read_affine_transformation(bio: BytesIO) -> AffineTransform:
    scale_x, skew_y, skew_x, scale_y, ip_x, ip_y = struct.unpack(
        "=dddddd", bio.read(8 * 6)
    )
    return AffineTransform(
        scale_x, skew_y, skew_x, scale_y, ip_x, ip_y, PixelAnchor.CENTER
    )


def _read_crs_wkt(bio: BytesIO) -> str:
    (size,) = struct.unpack("=i", bio.read(4))
    compressed_wkt = bio.read(size)
    crs_wkt = zlib.decompress(compressed_wkt)
    return crs_wkt.decode("utf-8")


def _read_sample_dimensions(
    bio: BytesIO,
) -> Tuple[List[SampleDimension], List[bytes]]:
    """Read band metadata and capture opaque category blobs.

    Returns
    -------
    Tuple of:
      - bands_meta: List[SampleDimension] — structured band metadata
      - category_blobs: List[bytes] — raw Kryo bytes per band (one blob per band)
    """
    (num_bands,) = struct.unpack("=i", bio.read(4))
    bands_meta = []
    category_blobs = []
    for i in range(num_bands):
        description = _read_utf8_string(bio)
        offset, scale, nodata = struct.unpack("=ddd", bio.read(8 * 3))
        blob = _read_java_object_blob(bio)
        category_blobs.append(blob)
        bands_meta.append(SampleDimension(description, offset, scale, nodata))
    return bands_meta, category_blobs


def _read_awt_raster(bio: BytesIO) -> Tuple[AWTRaster, bytes, bytes]:
    """Read AWT raster data and capture opaque property/colorModel blobs.

    Returns
    -------
    Tuple of:
      - awt_raster: AWTRaster — parsed raster data
      - properties_blob: bytes — raw Kryo bytes of image Hashtable
      - color_model_blob: bytes — raw Kryo bytes of ColorModelState
    """
    min_x, min_y, width, height = struct.unpack("=iiii", bio.read(4 * 4))
    properties_blob = _read_java_object_blob(bio)
    color_model_blob = _read_java_object_blob(bio)
    min_x_1, min_y_1 = struct.unpack("=ii", bio.read(4 * 2))
    if min_x_1 != min_x or min_y_1 != min_y:
        raise RuntimeError(
            "malformed serialized raster: minx/miny of the image cannot match "
            "with minx/miny of the AWT raster"
        )
    sample_model = _read_sample_model(bio)
    data_buffer = _read_data_buffer(bio)
    awt_raster = AWTRaster(min_x, min_y, width, height, sample_model, data_buffer)
    return awt_raster, properties_blob, color_model_blob


def _read_sample_model(bio: BytesIO) -> SampleModel:
    sample_model_type, data_type, width, height = struct.unpack(
        "=iiii", bio.read(4 * 4)
    )
    if sample_model_type == SampleModel.TYPE_BANDED:
        bank_indices = _read_int_array(bio)
        band_offsets = _read_int_array(bio)
        return ComponentSampleModel(
            data_type, width, height, 1, width, bank_indices, band_offsets
        )
    elif sample_model_type == SampleModel.TYPE_PIXEL_INTERLEAVED:
        pixel_stride, scanline_stride = struct.unpack("=ii", bio.read(4 * 2))
        band_offsets = _read_int_array(bio)
        return PixelInterleavedSampleModel(
            data_type, width, height, pixel_stride, scanline_stride, band_offsets
        )
    elif sample_model_type in [
        SampleModel.TYPE_COMPONENT,
        SampleModel.TYPE_COMPONENT_JAI,
    ]:
        pixel_stride, scanline_stride = struct.unpack("=ii", bio.read(4 * 2))
        bank_indices = _read_int_array(bio)
        band_offsets = _read_int_array(bio)
        return ComponentSampleModel(
            data_type,
            width,
            height,
            pixel_stride,
            scanline_stride,
            bank_indices,
            band_offsets,
        )
    elif sample_model_type == SampleModel.TYPE_SINGLE_PIXEL_PACKED:
        (scanline_stride,) = struct.unpack("=i", bio.read(4))
        bit_masks = _read_int_array(bio)
        return SinglePixelPackedSampleModel(
            data_type, width, height, scanline_stride, bit_masks
        )
    elif sample_model_type == SampleModel.TYPE_MULTI_PIXEL_PACKED:
        num_bits, scanline_stride, data_bit_offset = struct.unpack(
            "=iii", bio.read(4 * 3)
        )
        return MultiPixelPackedSampleModel(
            data_type, width, height, num_bits, scanline_stride, data_bit_offset
        )
    else:
        raise RuntimeError(f"Unsupported SampleModel type: {sample_model_type}")


def _read_data_buffer(bio: BytesIO) -> DataBuffer:
    (data_type,) = struct.unpack("=i", bio.read(4))
    offsets = _read_int_array(bio)
    (size,) = struct.unpack("=i", bio.read(4))

    (num_banks,) = struct.unpack("=i", bio.read(4))
    banks = []
    for i in range(num_banks):
        (bank_size,) = struct.unpack("=i", bio.read(4))
        if data_type == DataBuffer.TYPE_BYTE:
            np_array = np.frombuffer(bio.read(bank_size), dtype=np.uint8)
        elif data_type == DataBuffer.TYPE_SHORT:
            np_array = np.frombuffer(bio.read(2 * bank_size), dtype=np.int16)
        elif data_type == DataBuffer.TYPE_USHORT:
            np_array = np.frombuffer(bio.read(2 * bank_size), dtype=np.uint16)
        elif data_type == DataBuffer.TYPE_INT:
            np_array = np.frombuffer(bio.read(4 * bank_size), dtype=np.int32)
        elif data_type == DataBuffer.TYPE_FLOAT:
            np_array = np.frombuffer(bio.read(4 * bank_size), dtype=np.float32)
        elif data_type == DataBuffer.TYPE_DOUBLE:
            np_array = np.frombuffer(bio.read(8 * bank_size), dtype=np.float64)
        else:
            raise ValueError(f"unknown data_type {data_type}")

        banks.append(np_array)

    return DataBuffer(data_type, banks, size, offsets)


def _read_utf8_string(bio: BytesIO) -> str:
    (size,) = struct.unpack("=i", bio.read(4))
    utf8_bytes = bio.read(size)
    return utf8_bytes.decode("utf-8")


def _read_java_object_blob(bio: BytesIO) -> bytes:
    """Read a length-prefixed opaque Java object blob and return it.

    The format is: int32(size) + bytes(size).
    This replaces _ignore_java_object() for cases where we need to cache the blob.
    """
    (size,) = struct.unpack("=i", bio.read(4))
    return bio.read(size)


def _ignore_java_object(bio: BytesIO):
    (size,) = struct.unpack("=i", bio.read(4))
    bio.read(size)


def _read_int_array(bio: BytesIO) -> List[int]:
    (length,) = struct.unpack("=i", bio.read(4))
    return [struct.unpack("=i", bio.read(4))[0] for _ in range(length)]


def _read_utf8_string_map(bio: BytesIO) -> Optional[Dict[str, str]]:
    (size,) = struct.unpack("=i", bio.read(4))
    if size == -1:
        return None
    params = {}
    for _ in range(size):
        key = _read_utf8_string(bio)
        value = _read_utf8_string(bio)
        params[key] = value
    return params


def serialize(raster: "InDbSedonaRaster") -> Optional[bytes]:
    """Serialize an InDbSedonaRaster to the Sedona binary format.

    The output bytes are compatible with the JVM's Serde.deserialize().

    Parameters
    ----------
    raster : InDbSedonaRaster
        The raster to serialize. Must have cached blob fields
        (_category_blobs, _properties_blob, _color_model_blob).

    Returns
    -------
    Optional[bytes]
        The serialized raster in Sedona's binary format, or None if
        raster is None (SQL NULL passthrough).

    Raises
    ------
    TypeError
        If raster is not an InDbSedonaRaster.
    ValueError
        If raster is missing cached blob fields (was not created by
        deserialize() or with_bands()).
    """
    if raster is None:
        return None

    if not isinstance(raster, InDbSedonaRaster):
        raise TypeError(
            f"Cannot serialize {type(raster).__name__}. "
            "Only InDbSedonaRaster is supported."
        )

    # Validate required cached fields
    if (
        raster._category_blobs is None
        or raster._properties_blob is None
        or raster._color_model_blob is None
    ):
        raise ValueError(
            "Cannot serialize raster: missing cached blob fields. "
            "Only rasters created by deserialize() or with_bands() can be serialized."
        )

    bio = BytesIO()

    # 1. Type byte: IN_DB = 0
    bio.write(struct.pack("=b", RasterTypes.IN_DB))

    # 2. Name (length-prefixed UTF-8)
    _write_utf8_string(bio, raster._name)

    # 3. Grid envelope: width, height, x=0, y=0
    # The grid envelope origin (x, y) was applied via translate() during
    # deserialization and is now baked into the affine transform's ip_x/ip_y.
    # We always write x=0, y=0 on output.
    bio.write(struct.pack("=iiii", raster.width, raster.height, 0, 0))

    # 4. Affine transform (CENTER anchor convention, matching JVM)
    # The stored affine is UPPER_LEFT-anchored. Convert to CENTER.
    center_affine = raster.affine_trans.with_anchor(PixelAnchor.CENTER)
    bio.write(
        struct.pack(
            "=dddddd",
            center_affine.scale_x,
            center_affine.skew_y,
            center_affine.skew_x,
            center_affine.scale_y,
            center_affine.ip_x,
            center_affine.ip_y,
        )
    )

    # 5. CRS (zlib-compressed WKT)
    crs_bytes = raster.crs_wkt.encode("utf-8")
    compressed = zlib.compress(crs_bytes)
    bio.write(struct.pack("=i", len(compressed)))
    bio.write(compressed)

    # 6. Sample dimensions (band metadata + cached category blobs)
    _write_sample_dimensions(bio, raster.bands_meta, raster._category_blobs)

    # 7. Image header: minX, minY, width, height
    # Always write 0, 0 for minX/minY. The grid envelope origin was applied
    # via translate() during deserialization and is baked into the affine
    # transform's ip_x/ip_y. Writing the original awt.min_x/min_y here would
    # cause the JVM to apply the offset a second time, shifting the raster.
    awt = raster.awt_raster
    bio.write(struct.pack("=iiii", 0, 0, awt.width, awt.height))

    # 8. Properties blob (cached, length-prefixed)
    _write_java_object_blob(bio, raster._properties_blob)

    # 9. ColorModel blob (cached, length-prefixed)
    _write_java_object_blob(bio, raster._color_model_blob)

    # 10. AWT Raster: minX, minY (duplicated in format per JVM's
    #     DeepCopiedRenderedImage.write() then AWTRasterSerializer.write())
    # Must match image header (0, 0) — see Fix #1 comment above.
    bio.write(struct.pack("=ii", 0, 0))

    # 11. SampleModel
    _write_sample_model(bio, awt.sample_model)

    # 12. DataBuffer (the actual pixel data)
    _write_data_buffer(bio, awt.data_buffer)

    return bio.getvalue()


def _write_utf8_string(bio: BytesIO, s: str) -> None:
    """Write a length-prefixed UTF-8 string.

    Format: int32(len) + bytes(len)
    Matches JVM's KryoUtil.writeUTF8String().
    """
    encoded = s.encode("utf-8")
    bio.write(struct.pack("=i", len(encoded)))
    bio.write(encoded)


def _write_java_object_blob(bio: BytesIO, blob: bytes) -> None:
    """Write a length-prefixed opaque blob.

    Format: int32(size) + bytes(size)
    Matches JVM's KryoUtil.writeObjectWithLength().
    """
    bio.write(struct.pack("=i", len(blob)))
    bio.write(blob)


def _write_int_array(bio: BytesIO, arr: List[int]) -> None:
    """Write a length-prefixed int32 array.

    Format: int32(length) + length * int32(value)
    Matches JVM's KryoUtil.writeIntArray().
    """
    bio.write(struct.pack("=i", len(arr)))
    for v in arr:
        bio.write(struct.pack("=i", v))


def _write_sample_dimensions(
    bio: BytesIO,
    bands_meta: List[SampleDimension],
    category_blobs: List[bytes],
) -> None:
    """Write band metadata with cached category blobs.

    Format per band: utf8_string(description) + double(offset) + double(scale)
      + double(nodata) + length-prefixed blob(categories)
    """
    bio.write(struct.pack("=i", len(bands_meta)))
    for i, bm in enumerate(bands_meta):
        _write_utf8_string(bio, bm.description)
        bio.write(struct.pack("=ddd", bm.offset, bm.scale, bm.nodata))
        if i >= len(category_blobs):
            raise ValueError(
                f"Band {i} has no cached category blob. Expected {len(bands_meta)} "
                f"blobs but only {len(category_blobs)} were available. This indicates "
                f"a bug in with_bands() or deserialization."
            )
        _write_java_object_blob(bio, category_blobs[i])


def _write_sample_model(bio: BytesIO, sm: SampleModel) -> None:
    """Write a SampleModel in the format expected by JVM's SampleModelSerializer.

    Format: int32(type) + int32(dataType) + int32(width) + int32(height)
      + type-specific fields

    Supports all SampleModel types defined in SampleModel class constants.
    See SampleModelSerializer.java for the authoritative JVM format.
    """
    bio.write(
        struct.pack("=iiii", sm.sample_model_type, sm.data_type, sm.width, sm.height)
    )

    if sm.sample_model_type == SampleModel.TYPE_BANDED:
        # BandedSampleModel: bank_indices + band_offsets
        _write_int_array(bio, sm.bank_indices)
        _write_int_array(bio, sm.band_offsets)

    elif sm.sample_model_type == SampleModel.TYPE_PIXEL_INTERLEAVED:
        # PixelInterleavedSampleModel: pixel_stride + scanline_stride + band_offsets
        bio.write(struct.pack("=ii", sm.pixel_stride, sm.scanline_stride))
        _write_int_array(bio, sm.band_offsets)

    elif sm.sample_model_type in (
        SampleModel.TYPE_COMPONENT,
        SampleModel.TYPE_COMPONENT_JAI,
    ):
        # ComponentSampleModel: pixel_stride + scanline_stride + bank_indices + band_offsets
        bio.write(struct.pack("=ii", sm.pixel_stride, sm.scanline_stride))
        _write_int_array(bio, sm.bank_indices)
        _write_int_array(bio, sm.band_offsets)

    elif sm.sample_model_type == SampleModel.TYPE_SINGLE_PIXEL_PACKED:
        # SinglePixelPackedSampleModel: scanline_stride + bit_masks
        bio.write(struct.pack("=i", sm.scanline_stride))
        _write_int_array(bio, sm.bit_masks)

    elif sm.sample_model_type == SampleModel.TYPE_MULTI_PIXEL_PACKED:
        # MultiPixelPackedSampleModel: num_bits + scanline_stride + data_bit_offset
        bio.write(
            struct.pack("=iii", sm.num_bits, sm.scanline_stride, sm.data_bit_offset)
        )

    else:
        raise RuntimeError(f"Unsupported SampleModel type: {sm.sample_model_type}")


def _write_data_buffer(bio: BytesIO, db: DataBuffer) -> None:
    """Write a DataBuffer in the format expected by JVM's DataBufferSerializer.

    Format: int32(dataType) + int_array(offsets) + int32(size) + int32(numBanks)
      + per bank: int32(bankSize) + typed_array(data)

    The typed_array format depends on dataType:
      TYPE_BYTE:   bankSize bytes (1 byte each)
      TYPE_SHORT:  bankSize * 2 bytes (int16)
      TYPE_USHORT: bankSize * 2 bytes (uint16)
      TYPE_INT:    bankSize * 4 bytes (int32)
      TYPE_FLOAT:  bankSize * 4 bytes (float32)
      TYPE_DOUBLE: bankSize * 8 bytes (float64)
    """
    bio.write(struct.pack("=i", db.data_type))
    _write_int_array(bio, db.offsets)
    bio.write(struct.pack("=i", db.size))

    num_banks = len(db.bank_data)
    bio.write(struct.pack("=i", num_banks))
    for bank in db.bank_data:
        bank_size = len(bank)
        bio.write(struct.pack("=i", bank_size))
        # Use memoryview to avoid copying the entire bank into a new bytes
        # object. np.ascontiguousarray ensures the memoryview is valid.
        contiguous = np.ascontiguousarray(bank)
        bio.write(memoryview(contiguous))
