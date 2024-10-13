#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

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
    bands_meta = _read_sample_dimensions(bio)
    if raster_type == RasterTypes.IN_DB:
        # In-DB raster
        awt_raster = _read_awt_raster(bio)
        return InDbSedonaRaster(
            width, height, bands_meta, affine_trans, crs_wkt, awt_raster
        )
    else:
        raise ValueError("unsupported raster_type: {}".format(raster_type))


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


def _read_sample_dimensions(bio: BytesIO) -> List[SampleDimension]:
    (num_bands,) = struct.unpack("=i", bio.read(4))
    bands_meta = []
    for i in range(num_bands):
        description = _read_utf8_string(bio)
        offset, scale, nodata = struct.unpack("=ddd", bio.read(8 * 3))
        _ignore_java_object(bio)
        bands_meta.append(SampleDimension(description, offset, scale, nodata))
    return bands_meta


def _read_awt_raster(bio: BytesIO) -> AWTRaster:
    min_x, min_y, width, height = struct.unpack("=iiii", bio.read(4 * 4))
    _ignore_java_object(bio)  # image properties
    _ignore_java_object(bio)  # color model
    min_x_1, min_y_1 = struct.unpack("=ii", bio.read(4 * 2))
    if min_x_1 != min_x or min_y_1 != min_y:
        raise RuntimeError(
            "malformed serialized raster: minx/miny of the image cannot match with minx/miny of the AWT raster"
        )
    sample_model = _read_sample_model(bio)
    data_buffer = _read_data_buffer(bio)
    return AWTRaster(min_x, min_y, width, height, sample_model, data_buffer)


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
            raise ValueError("unknown data_type {}".format(data_type))

        banks.append(np_array)

    return DataBuffer(data_type, banks, size, offsets)


def _read_utf8_string(bio: BytesIO) -> str:
    (size,) = struct.unpack("=i", bio.read(4))
    utf8_bytes = bio.read(size)
    return utf8_bytes.decode("utf-8")


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
