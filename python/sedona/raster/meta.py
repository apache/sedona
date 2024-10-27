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

from enum import Enum
from typing import Dict, List, Optional


class PixelAnchor(Enum):
    """Anchor of the pixel cell. GeoTools anchors the coordinates at the center
    of pixels, while GDAL anchors the coordinates at the upper-left corner of
    the pixels. This difference requires us to convert the affine
    transformation between these conventions.

    """

    CENTER = 1
    UPPER_LEFT = 2


class AffineTransform:
    scale_x: float
    skew_y: float
    skew_x: float
    scale_y: float
    ip_x: float
    ip_y: float
    pixel_anchor: PixelAnchor

    def __init__(
        self, scale_x, skew_y, skew_x, scale_y, ip_x, ip_y, pixel_anchor: PixelAnchor
    ):
        self.scale_x = scale_x
        self.skew_y = skew_y
        self.skew_x = skew_x
        self.scale_y = scale_y
        self.ip_x = ip_x
        self.ip_y = ip_y
        self.pixel_anchor = pixel_anchor

    def with_anchor(self, pixel_anchor: PixelAnchor):
        if pixel_anchor == self.pixel_anchor:
            return self
        return self._do_change_pixel_anchor(self.pixel_anchor, pixel_anchor)

    def translate(self, offset_x: float, offset_y: float):
        new_ipx = self.ip_x + offset_x * self.scale_x + offset_y * self.skew_x
        new_ipy = self.ip_y + offset_x * self.skew_y + offset_y * self.scale_y
        return AffineTransform(
            self.scale_x,
            self.skew_y,
            self.skew_x,
            self.scale_y,
            new_ipx,
            new_ipy,
            self.pixel_anchor,
        )

    def _do_change_pixel_anchor(self, from_anchor: PixelAnchor, to_anchor: PixelAnchor):
        assert from_anchor != to_anchor
        if from_anchor == PixelAnchor.CENTER:
            m00 = 1.0
            m10 = 0.0
            m01 = 0.0
            m11 = 1.0
            m02 = -0.5
            m12 = -0.5
        else:
            m00 = 1.0
            m10 = 0.0
            m01 = 0.0
            m11 = 1.0
            m02 = 0.5
            m12 = 0.5

        old_m00 = self.scale_x
        old_m10 = self.skew_y
        old_m01 = self.skew_x
        old_m11 = self.scale_y
        old_m02 = self.ip_x
        old_m12 = self.ip_y
        new_m00 = old_m00 * m00 + old_m01 * m10
        new_m01 = old_m00 * m01 + old_m01 * m11
        new_m02 = old_m00 * m02 + old_m01 * m12 + old_m02
        new_m10 = old_m10 * m00 + old_m11 * m10
        new_m11 = old_m10 * m01 + old_m11 * m11
        new_m12 = old_m10 * m02 + old_m11 * m12 + old_m12
        return AffineTransform(
            new_m00, new_m10, new_m01, new_m11, new_m02, new_m12, to_anchor
        )

    def __repr__(self):
        return (
            "[ {} {} {}\n".format(self.scale_x, self.skew_x, self.ip_x)
            + "  {} {} {}\n".format(self.skew_y, self.scale_y, self.ip_y)
            + "   0  0  1 ]"
        )


class SampleDimension:
    """Raster band metadata."""

    description: str
    offset: float
    scale: float
    nodata: float

    def __init__(self, description, offset, scale, nodata):
        self.description = description
        self.offset = offset
        self.scale = scale
        self.nodata = nodata
