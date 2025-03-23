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
        if self.scanline_stride == self.width and self.pixel_stride == 1:
            # Fast path: no gaps between pixels
            band_arrs = []
            for bank_index in self.bank_indices:
                bank_data = data_buffer.bank_data[bank_index]
                offset = self.band_offsets[bank_index]
                if offset != 0:
                    bank_data = bank_data[offset : (offset + self.width * self.height)]
                band_arr = bank_data.reshape(self.height, self.width)
                band_arrs.append(band_arr)
            return np.array(band_arrs)
        else:
            # Slow path
            band_arrs = []
            for k in range(len(self.bank_indices)):
                bank_index = self.bank_indices[k]
                bank_data = data_buffer.bank_data[bank_index]
                offset = self.band_offsets[k]
                band_pixel_data = []
                for y in range(self.height):
                    for x in range(self.width):
                        pos = offset + y * self.scanline_stride + x * self.pixel_stride
                        band_pixel_data.append(bank_data[pos])
                arr = np.array(band_pixel_data).reshape(self.height, self.width)
                band_arrs.append(arr)

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
        bank_data = data_buffer.bank_data[0]
        if (
            self.pixel_stride == num_bands
            and self.scanline_stride == self.width * num_bands
            and self.band_offsets == list(range(0, num_bands))
        ):
            # Fast path: no gapping in between band data, no band reordering
            arr = bank_data.reshape(self.height, self.width, num_bands)
            return np.transpose(arr, [2, 0, 1])
        else:
            # Slow path
            pixel_data = []
            for y in range(self.height):
                for x in range(self.width):
                    begin = y * self.scanline_stride + x * self.pixel_stride
                    end = begin + num_bands
                    pixel = bank_data[begin:end][self.band_offsets]
                    pixel_data.append(pixel)
            arr = np.array(pixel_data).reshape(self.height, self.width, num_bands)
            return np.transpose(arr, [2, 0, 1])


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
            self.bit_offsets.append((v & -v).bit_length() - 1)

    def as_numpy(self, data_buffer: DataBuffer) -> np.ndarray:
        num_bands = len(self.bit_masks)
        bank_data = data_buffer.bank_data[0]
        pixel_data = []
        for y in range(self.height):
            for x in range(self.width):
                pos = y * self.scanline_stride + x
                value = bank_data[pos]
                pixel = []
                for mask, bit_offset in zip(self.bit_masks, self.bit_offsets):
                    pixel.append((value & mask) >> bit_offset)
                pixel_data.append(pixel)
        arr = np.array(pixel_data, dtype=bank_data.dtype).reshape(
            self.height, self.width, num_bands
        )
        return np.transpose(arr, [2, 0, 1])


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
        bank_data = data_buffer.bank_data[0]
        bits_per_value = bank_data.dtype.itemsize * 8
        pixel_per_value = bits_per_value / self.num_bits
        shift_right = bits_per_value - self.num_bits
        mask = ((1 << self.num_bits) - 1) << shift_right

        band_data = []
        for y in range(self.height):
            pos = y * self.scanline_stride + self.data_bit_offset // bits_per_value
            value = bank_data[pos]
            shift = self.data_bit_offset % bits_per_value
            value = value << shift
            pixels: List[int] = []
            while len(pixels) < self.width:
                while shift < bits_per_value and len(pixels) < self.width:
                    pixels.append((value & mask) >> shift_right)
                    value = value << self.num_bits
                    shift += self.num_bits
                pos += 1
                value = bank_data[pos]
                shift = 0
            band_data.append(np.array(pixels, dtype=bank_data.dtype))

        return np.array(band_data).reshape(1, self.height, self.width)
