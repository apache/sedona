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

from .data_buffer import DataBuffer
from .sample_model import SampleModel


class AWTRaster:
    """Raster data structure of Java AWT Raster used by GeoTools GridCoverage2D."""

    min_x: int
    min_y: int
    width: int
    height: int
    sample_model: SampleModel
    data_buffer: DataBuffer

    def __init__(
        self,
        min_x,
        min_y,
        width,
        height,
        sample_model: SampleModel,
        data_buffer: DataBuffer,
    ):
        if sample_model.width != width or sample_model.height != height:
            raise RuntimeError("Size of the image does not match with the sample model")
        self.min_x = min_x
        self.min_y = min_y
        self.width = width
        self.height = height
        self.sample_model = sample_model
        self.data_buffer = data_buffer
