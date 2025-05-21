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

from sedona.spark.stats.outlier_detection.local_outlier_factor import (
    local_outlier_factor,
)

import warnings

warnings.warn(
    "The 'sedona.stats.outlier_detection.local_outlier_factor' module is deprecated and will be removed in future versions. Please use 'sedona.spark.stats' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["local_outlier_factor"]
