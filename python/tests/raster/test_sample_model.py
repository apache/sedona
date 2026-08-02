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

"""Tests for reading rasters whose sample models use the less common layouts.

The expected samples of every test were produced by handing the same sample model and data
buffer to java.awt.image, so they record what Java AWT reads for these layouts. Each test
names the Java layout it covers, which is enough to reproduce it.
"""

import warnings
from typing import List

import numpy as np
import pytest

from sedona.spark.raster.data_buffer import DataBuffer
from sedona.spark.raster.sample_model import (
    ComponentSampleModel,
    MultiPixelPackedSampleModel,
    PixelInterleavedSampleModel,
    SinglePixelPackedSampleModel,
)


def ramp(start: int, length: int, dtype=np.int32) -> np.ndarray:
    """A bank of consecutive values, so that a sample identifies its own position."""
    return np.arange(start, start + length, dtype=dtype)


def signed_int32(mask: int) -> int:
    """Turn a bit mask written in its unsigned form into the value the deserializer
    produces for it, which is negative for masks covering the top bit."""
    return mask - (1 << 32) if mask >= (1 << 31) else mask


def int_bank(values: List[int]) -> np.ndarray:
    """An int32 bank holding values written in their unsigned form."""
    return np.array(values, dtype=np.uint32).astype(np.int32)


def test_component_fast_path_pairs_band_offsets_with_bands() -> None:
    # ComponentSampleModel(TYPE_INT, 2, 2, 1, 2, {1, 0}, {3, 1})
    sample_model = ComponentSampleModel(DataBuffer.TYPE_INT, 2, 2, 1, 2, [1, 0], [3, 1])
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [ramp(10, 7), ramp(20, 7)], 7, [0, 0])

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[23, 24], [25, 26]], [[11, 12], [13, 14]]]),
    )


def test_component_fast_path_applies_bank_offsets() -> None:
    # ComponentSampleModel(TYPE_INT, 2, 2, 1, 2, {1, 0}, {3, 1})
    # over DataBufferInt(banks, 7, {2, 1})
    sample_model = ComponentSampleModel(DataBuffer.TYPE_INT, 2, 2, 1, 2, [1, 0], [3, 1])
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [ramp(10, 9), ramp(20, 9)], 7, [2, 1])

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[24, 25], [26, 27]], [[13, 14], [15, 16]]]),
    )


def test_component_fast_path_reads_a_bounded_window_of_padded_banks() -> None:
    # ComponentSampleModel(TYPE_INT, 2, 2, 1, 2, {0, 1}, {0, 0}) over banks of 6 samples,
    # so the band offsets alone give no reason to narrow a bank down to 4 samples
    sample_model = ComponentSampleModel(DataBuffer.TYPE_INT, 2, 2, 1, 2, [0, 1], [0, 0])
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [ramp(10, 6), ramp(20, 6)], 6, [0, 0])

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[10, 11], [12, 13]], [[20, 21], [22, 23]]]),
    )


def test_banded_sample_model_over_reordered_banks() -> None:
    # BandedSampleModel(TYPE_INT, 2, 2, 2, {1, 0}, {1, 2}) over DataBufferInt(banks, 6, {1, 2}),
    # which is deserialized as a component model with a unit pixel stride
    sample_model = ComponentSampleModel(DataBuffer.TYPE_INT, 2, 2, 1, 2, [1, 0], [1, 2])
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [ramp(10, 8), ramp(20, 8)], 6, [1, 2])

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[23, 24], [25, 26]], [[13, 14], [15, 16]]]),
    )


def test_component_slow_path_applies_bank_offsets() -> None:
    # ComponentSampleModel(TYPE_INT, 3, 2, 2, 8, {1, 1, 0}, {0, 1, 4})
    # over DataBufferInt(banks, 20, {3, 2}): two bands share a bank, and there are gaps
    # both between the scanlines and between the pixels within them
    sample_model = ComponentSampleModel(
        DataBuffer.TYPE_INT, 3, 2, 2, 8, [1, 1, 0], [0, 1, 4]
    )
    data_buffer = DataBuffer(
        DataBuffer.TYPE_INT, [ramp(100, 24), ramp(200, 24)], 20, [3, 2]
    )

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array(
            [
                [[202, 204, 206], [210, 212, 214]],
                [[203, 205, 207], [211, 213, 215]],
                [[107, 109, 111], [115, 117, 119]],
            ]
        ),
    )


def test_pixel_interleaved_fast_path_applies_bank_offset() -> None:
    # PixelInterleavedSampleModel(TYPE_INT, 2, 2, 3, 6, {0, 1, 2})
    # over DataBufferInt(bank of 20 samples, 17, {3})
    sample_model = PixelInterleavedSampleModel(
        DataBuffer.TYPE_INT, 2, 2, 3, 6, [0, 1, 2]
    )
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [ramp(10, 20)], 17, [3])

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[13, 16], [19, 22]], [[14, 17], [20, 23]], [[15, 18], [21, 24]]]),
    )


def test_pixel_interleaved_slow_path_band_offsets_may_exceed_pixel_stride() -> None:
    # PixelInterleavedSampleModel(TYPE_INT, 2, 2, 4, 10, {3, 1, 0})
    # over DataBufferInt(bank, 20, {2}): band 0 sits past the window the three bands of a
    # single pixel would occupy
    sample_model = PixelInterleavedSampleModel(
        DataBuffer.TYPE_INT, 2, 2, 4, 10, [3, 1, 0]
    )
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [ramp(10, 24)], 20, [2])

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[15, 19], [25, 29]], [[13, 17], [23, 27]], [[12, 16], [22, 26]]]),
    )


def test_single_pixel_packed_argb_does_not_sign_extend() -> None:
    # SinglePixelPackedSampleModel(TYPE_INT, 2, 2, 3, {0xFF000000, 0xFF0000, 0xFF00, 0xFF})
    # over DataBufferInt(bank, 7, {1}): the alpha mask covers the sign bit of the samples
    sample_model = SinglePixelPackedSampleModel(
        DataBuffer.TYPE_INT,
        2,
        2,
        3,
        [signed_int32(0xFF000000), 0x00FF0000, 0x0000FF00, 0x000000FF],
    )
    data_buffer = DataBuffer(
        DataBuffer.TYPE_INT,
        [int_bank([0, 0x8090A0B0, 0x102030F0, 0, 0xFFFFFFFF, 0x01020304, 0, 0])],
        7,
        [1],
    )

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array(
            [
                [[128, 16], [255, 1]],
                [[144, 32], [255, 2]],
                [[160, 48], [255, 3]],
                [[176, 240], [255, 4]],
            ]
        ),
    )


def test_single_pixel_packed_zero_mask_reads_zero() -> None:
    # SinglePixelPackedSampleModel(TYPE_INT, 2, 1, 2, {0xFF0000, 0xFF00, 0xFF, 0}): Java
    # accepts a zero mask, leaves its bit offset at zero and reads the band as zero
    sample_model = SinglePixelPackedSampleModel(
        DataBuffer.TYPE_INT, 2, 1, 2, [0x00FF0000, 0x0000FF00, 0x000000FF, 0]
    )
    assert sample_model.bit_offsets == [16, 8, 0, 0]
    data_buffer = DataBuffer(
        DataBuffer.TYPE_INT, [int_bank([0x8090A0B0, 0xFFFFFFFF])], 2, [0]
    )

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[144, 255]], [[160, 255]], [[176, 255]], [[0, 0]]]),
    )


def test_single_pixel_packed_applies_bank_offset() -> None:
    # SinglePixelPackedSampleModel(TYPE_USHORT, 2, 2, 4, {0xF800, 0x7E0, 0x1F}) over
    # DataBufferUShort(bank, 8, {2}): RGB 565 pixels starting two samples into the bank
    sample_model = SinglePixelPackedSampleModel(
        DataBuffer.TYPE_USHORT, 2, 2, 4, [0xF800, 0x07E0, 0x001F]
    )
    data_buffer = DataBuffer(
        DataBuffer.TYPE_USHORT,
        [np.array([0, 0xF81F, 0x07E0, 0, 0, 0x1234, 0xFFFF, 0], dtype=np.uint16)],
        8,
        [2],
    )

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[0, 0], [31, 0]], [[63, 0], [63, 0]], [[0, 0], [31, 0]]]),
    )


def test_multi_pixel_packed_applies_bit_and_bank_offsets() -> None:
    # MultiPixelPackedSampleModel(TYPE_BYTE, 5, 2, 4, 4, 4) over DataBufferByte(bank, 9, {1}):
    # four bits per pixel, with both the bank and the first scanline starting half a byte in
    sample_model = MultiPixelPackedSampleModel(DataBuffer.TYPE_BYTE, 5, 2, 4, 4, 4)
    data_buffer = DataBuffer(
        DataBuffer.TYPE_BYTE,
        [
            np.array(
                [0xFF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x00],
                dtype=np.uint8,
            )
        ],
        9,
        [1],
    )

    # an ordinary packed layout, so reading it must not warn about Java's shift quirks
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        arr = sample_model.as_numpy(data_buffer)

    np.testing.assert_array_equal(
        arr, np.array([[[1, 2, 3, 4, 5], [9, 10, 11, 12, 13]]])
    )


def test_multi_pixel_packed_stays_within_the_bank() -> None:
    # MultiPixelPackedSampleModel(TYPE_BYTE, 8, 1, 1, 1, 0) over DataBufferByte(bank, 1, {0}):
    # the whole image is held in a single byte, so reading beyond it is out of bounds
    sample_model = MultiPixelPackedSampleModel(DataBuffer.TYPE_BYTE, 8, 1, 1, 1, 0)
    data_buffer = DataBuffer(
        DataBuffer.TYPE_BYTE, [np.array([0xA9], dtype=np.uint8)], 1, [0]
    )

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[1, 0, 1, 0, 1, 0, 0, 1]]]),
    )


def test_multi_pixel_packed_scanline_spanning_several_samples() -> None:
    # MultiPixelPackedSampleModel(TYPE_BYTE, 5, 2, 2, 3, 6) over DataBufferByte(bank, 7, {1}):
    # scanlines of two bit pixels that start near the end of a byte
    sample_model = MultiPixelPackedSampleModel(DataBuffer.TYPE_BYTE, 5, 2, 2, 3, 6)
    data_buffer = DataBuffer(
        DataBuffer.TYPE_BYTE,
        [np.array([0x00, 0x1B, 0xE4, 0x39, 0x00, 0x4E, 0x93, 0x00], dtype=np.uint8)],
        7,
        [1],
    )

    np.testing.assert_array_equal(
        sample_model.as_numpy(data_buffer),
        np.array([[[3, 3, 2, 1, 0], [0, 1, 0, 3, 2]]]),
    )


@pytest.mark.parametrize(
    "bank,expected",
    [
        # the top sample has its sign bit set, so Java's arithmetic shift extends it
        ([0x89ABCDEF, 0x12345678], [154, 188, 222, 248]),
        # and a positive sample, where the shifted-down top bits are not sign bits
        ([0x12345678, 0], [35, 69, 103, 1]),
    ],
)
def test_multi_pixel_packed_pixel_straddling_two_samples(
    bank: List[int], expected: List[int]
) -> None:
    # MultiPixelPackedSampleModel(TYPE_INT, 4, 1, 8, 1, 4): a data bit offset that is not a
    # multiple of num_bits leaves the last pixel straddling two samples, and Java's shift
    # distance for it goes negative. Java takes an int shift distance modulo 32, so -4 shifts
    # right by 28 and reads the top bits of the sample rather than shifting it all out.
    sample_model = MultiPixelPackedSampleModel(DataBuffer.TYPE_INT, 4, 1, 8, 1, 4)
    data_buffer = DataBuffer(DataBuffer.TYPE_INT, [int_bank(bank)], 2, [0])

    with pytest.warns(UserWarning, match="straddle two samples"):
        arr = sample_model.as_numpy(data_buffer)

    np.testing.assert_array_equal(arr, np.array([[expected]]))


def test_multi_pixel_packed_whole_sample_pixels_read_zero() -> None:
    # MultiPixelPackedSampleModel(TYPE_INT, 3, 2, 32, 4, 0): Java accepts a pixel that
    # occupies a whole 32 bit sample, but derives a zero bit mask for it from
    # `(1 << 32) - 1`, so it reads every pixel of such a raster as zero
    sample_model = MultiPixelPackedSampleModel(DataBuffer.TYPE_INT, 3, 2, 32, 4, 0)
    data_buffer = DataBuffer(
        DataBuffer.TYPE_INT,
        [
            int_bank(
                [
                    0x11223344,
                    0x55667788,
                    0x99AABBCC,
                    0,
                    0xDEADBEEF,
                    0x0F0F0F0F,
                    0x12345678,
                    0,
                ]
            )
        ],
        8,
        [0],
    )

    with pytest.warns(UserWarning, match="one pixel per 32 bit sample"):
        arr = sample_model.as_numpy(data_buffer)

    np.testing.assert_array_equal(arr, np.array([[[0, 0, 0], [0, 0, 0]]]))


@pytest.mark.parametrize(
    "data_type,dtype",
    [
        (DataBuffer.TYPE_BYTE, np.uint8),
        (DataBuffer.TYPE_SHORT, np.int16),
        (DataBuffer.TYPE_USHORT, np.uint16),
        (DataBuffer.TYPE_INT, np.int32),
        (DataBuffer.TYPE_FLOAT, np.float32),
        (DataBuffer.TYPE_DOUBLE, np.float64),
    ],
)
def test_component_sample_model_keeps_the_data_type(data_type: int, dtype) -> None:
    contiguous = ComponentSampleModel(data_type, 2, 2, 1, 2, [0], [1])
    strided = ComponentSampleModel(data_type, 2, 2, 2, 5, [0], [1])

    arr = contiguous.as_numpy(DataBuffer(data_type, [ramp(1, 6, dtype)], 5, [1]))
    assert arr.dtype == dtype
    np.testing.assert_array_equal(arr, np.array([[[3, 4], [5, 6]]], dtype=dtype))

    arr = strided.as_numpy(DataBuffer(data_type, [ramp(1, 12, dtype)], 11, [1]))
    assert arr.dtype == dtype
    np.testing.assert_array_equal(arr, np.array([[[3, 5], [8, 10]]], dtype=dtype))
