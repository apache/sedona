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

from abc import ABC, abstractmethod
from typing import List

import numpy as np

from .data_buffer import DataBuffer


class SampleModel(ABC):
    """The SampleModel class and its subclasses are defined according to the data structure of
    SampleModel class in Java AWT.

    """

    TYPE_BANDED = 1
    TYPE_PIXEL_INTERLEAVED = 2
    TYPE_SINGLE_PIXEL_PACKED = 3
    TYPE_MULTI_PIXEL_PACKED = 4
    TYPE_COMPONENT_JAI = 5
    TYPE_COMPONENT = 6

    sample_model_type: int
    data_type: int
    width: int
    height: int
    scanline_stride: int

    def __init__(self, sample_model_type, data_type, width, height):
        self.sample_model_type = sample_model_type
        self.data_type = data_type
        self.width = width
        self.height = height

    @abstractmethod
    def as_numpy(self, data_buffer: DataBuffer) -> np.ndarray:
        raise NotImplementedError(
            "Abstract method as_numpy was not implemented by subclass"
        )

    def _pixel_positions(self, pixel_stride: int, offset: int = 0) -> np.ndarray:
        """Sample position of every pixel, as a (height, width) array of indices.

        The positions are relative to the start of a bank, so they index the arrays
        returned by :meth:`DataBuffer.bank_samples`.
        """
        rows = np.arange(self.height) * self.scanline_stride + offset
        cols = np.arange(self.width) * pixel_stride
        return rows[:, np.newaxis] + cols[np.newaxis, :]


class ComponentSampleModel(SampleModel):
    pixel_stride: int
    scanline_stride: int
    bank_indices: List[int]
    band_offsets: List[int]

    def __init__(
        self,
        data_type,
        width,
        height,
        pixel_stride,
        scanline_stride,
        bank_indices,
        band_offsets,
    ):
        super().__init__(SampleModel.TYPE_COMPONENT, data_type, width, height)
        self.pixel_stride = pixel_stride
        self.scanline_stride = scanline_stride
        self.bank_indices = bank_indices
        self.band_offsets = band_offsets

    def as_numpy(self, data_buffer: DataBuffer) -> np.ndarray:
        contiguous = self.scanline_stride == self.width and self.pixel_stride == 1
        num_samples = self.width * self.height

        band_arrs = []
        for k, bank_index in enumerate(self.bank_indices):
            # The band offset is relative to the offset of the bank the band lives in, and
            # bank_indices/band_offsets are both indexed by band, not by bank.
            samples = data_buffer.bank_samples(bank_index)
            offset = self.band_offsets[k]
            if contiguous:
                # Fast path: the samples of a band follow each other. The bank may still
                # hold more samples than the band needs, so slice a bounded window.
                band_arr = samples[offset : (offset + num_samples)].reshape(
                    self.height, self.width
                )
            else:
                # Slow path: gaps between pixels or scanlines
                band_arr = samples[self._pixel_positions(self.pixel_stride, offset)]
            band_arrs.append(band_arr)

        return np.array(band_arrs)


class PixelInterleavedSampleModel(SampleModel):
    pixel_stride: int
    scanline_stride: int
    band_offsets: List[int]

    def __init__(
        self, data_type, width, height, pixel_stride, scanline_stride, band_offsets
    ):
        super().__init__(SampleModel.TYPE_PIXEL_INTERLEAVED, data_type, width, height)
        self.pixel_stride = pixel_stride
        self.scanline_stride = scanline_stride
        self.band_offsets = band_offsets

    def as_numpy(self, data_buffer: DataBuffer) -> np.ndarray:
        num_bands = len(self.band_offsets)
        samples = data_buffer.bank_samples()
        if (
            self.pixel_stride == num_bands
            and self.scanline_stride == self.width * num_bands
            and self.band_offsets == list(range(0, num_bands))
        ):
            # Fast path: no gapping in between band data, no band reordering. The bank may
            # still hold more samples than the image needs, so slice a bounded window.
            num_samples = self.width * self.height * num_bands
            arr = samples[:num_samples].reshape(self.height, self.width, num_bands)
            return np.transpose(arr, [2, 0, 1])
        else:
            # Slow path. Band offsets are positions within a scanline, so they are not
            # bound to the pixel they belong to and may reach past its pixel stride.
            positions = self._pixel_positions(self.pixel_stride)
            band_arrs = [samples[positions + offset] for offset in self.band_offsets]
            return np.array(band_arrs)


class SinglePixelPackedSampleModel(SampleModel):
    scanline_stride: int
    bit_masks: List[int]
    bit_offsets: List[int]

    def __init__(self, data_type, width, height, scanline_stride, bit_masks):
        super().__init__(SampleModel.TYPE_SINGLE_PIXEL_PACKED, data_type, width, height)
        self.scanline_stride = scanline_stride
        self.bit_masks = bit_masks
        self.bit_offsets = []
        for v in self.bit_masks:
            # Java leaves the bit offset of a zero mask at zero, and reads such a band as
            # zero. Deriving it from the mask would give -1 for it.
            self.bit_offsets.append((v & -v).bit_length() - 1 if v else 0)

    def as_numpy(self, data_buffer: DataBuffer) -> np.ndarray:
        samples = data_buffer.bank_samples()
        # Java extracts the bands with `(value & mask) >>> bitOffset`. Read the samples as
        # unsigned so that a mask covering the sign bit, such as the alpha mask of an ARGB
        # raster, does not sign-extend into the band values.
        unsigned_dtype = np.dtype(f"u{samples.dtype.itemsize}")
        values = samples[self._pixel_positions(1)].astype(unsigned_dtype)
        # The bit masks are deserialized as signed 32 bit integers, so masks covering the
        # sign bit arrive negative. Take their two's complement bits.
        value_mask = (1 << (samples.dtype.itemsize * 8)) - 1
        band_arrs = [
            (values & unsigned_dtype.type(mask & value_mask)) >> bit_offset
            for mask, bit_offset in zip(self.bit_masks, self.bit_offsets)
        ]
        return np.array(band_arrs).astype(samples.dtype)


class MultiPixelPackedSampleModel(SampleModel):
    num_bits: int
    scanline_stride: int
    data_bit_offset: int

    def __init__(
        self, data_type, width, height, num_bits, scanline_stride, data_bit_offset
    ):
        super().__init__(SampleModel.TYPE_MULTI_PIXEL_PACKED, data_type, width, height)
        self.num_bits = num_bits
        self.scanline_stride = scanline_stride
        self.data_bit_offset = data_bit_offset

    def as_numpy(self, data_buffer: DataBuffer) -> np.ndarray:
        samples = data_buffer.bank_samples()
        bits_per_value = samples.dtype.itemsize * 8

        # Resolve every pixel on its own, the way Java does: this sample model requires
        # num_bits to divide the size of a data element, so no pixel spans two elements.
        pixel_bits = self.data_bit_offset + np.arange(self.width) * self.num_bits
        cols = pixel_bits // bits_per_value
        shifts = bits_per_value - (pixel_bits % bits_per_value) - self.num_bits

        rows = np.arange(self.height) * self.scanline_stride
        positions = rows[:, np.newaxis] + cols[np.newaxis, :]
        # Shift the samples as unsigned values, otherwise pixels held in the sign bit of a
        # signed data type would sign-extend instead of shifting in zeroes.
        unsigned_dtype = np.dtype(f"u{samples.dtype.itemsize}")
        values = samples[positions].astype(unsigned_dtype)
        # Java derives this mask as `(1 << numberOfBits) - 1` on an int, and the shift count
        # of an int shift is taken modulo 32. A pixel occupying a whole 32 bit sample
        # therefore ends up with a zero mask, and Java reads every such pixel as zero.
        bit_mask = (1 << (self.num_bits % 32)) - 1
        pixels = (values >> shifts) & bit_mask

        return pixels.astype(samples.dtype).reshape(1, self.height, self.width)
