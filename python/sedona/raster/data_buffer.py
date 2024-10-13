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

from typing import Any, List

import numpy as np


class DataBuffer:
    TYPE_BYTE = 0
    TYPE_USHORT = 1
    TYPE_SHORT = 2
    TYPE_INT = 3
    TYPE_FLOAT = 4
    TYPE_DOUBLE = 5

    data_type: int
    bank_data: List[np.ndarray]
    size: int
    offsets: List[int]

    def __init__(
        self, data_type: int, bank_data: List[np.ndarray], size: int, offsets: List[int]
    ):
        self.data_type = data_type
        self.bank_data = bank_data
        self.size = size
        self.offsets = offsets
