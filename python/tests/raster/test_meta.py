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

import pytest
from pytest import approx

from sedona.raster.meta import AffineTransform
from sedona.raster.meta import PixelAnchor


class TestAffineTransform:

    def test_change_anchor_to_upper_left(self):
        scale_x = 10.0
        skew_y = 1.0
        skew_x = 2.0
        scale_y = -8.0
        ip_x = 100
        ip_y = 200

        trans = AffineTransform(scale_x, skew_y, skew_x, scale_y, ip_x, ip_y, PixelAnchor.CENTER)
        trans_gdal = trans.with_anchor(PixelAnchor.UPPER_LEFT)
        assert trans_gdal.scale_x == approx(scale_x)
        assert trans_gdal.scale_y == approx(scale_y)
        assert trans_gdal.skew_x == approx(skew_x)
        assert trans_gdal.skew_y == approx(skew_y)
        assert trans_gdal.ip_x == approx(94.0)
        assert trans_gdal.ip_y == approx(203.5)

    def test_change_anchor_to_center(self):
        scale_x = 10.0
        skew_y = 1.0
        skew_x = 2.0
        scale_y = -8.0
        ip_x = 100
        ip_y = 200

        trans_gdal = AffineTransform(scale_x, skew_y, skew_x, scale_y, ip_x, ip_y, PixelAnchor.UPPER_LEFT)
        trans = trans_gdal.with_anchor(PixelAnchor.CENTER)
        assert trans.scale_x == approx(scale_x)
        assert trans.scale_y == approx(scale_y)
        assert trans.skew_x == approx(skew_x)
        assert trans.skew_y == approx(skew_y)
        assert trans.ip_x == approx(106.0)
        assert trans.ip_y == approx(196.5)
