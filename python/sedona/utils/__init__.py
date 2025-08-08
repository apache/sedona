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
import warnings
from sedona.spark import utils


def __getattr__(name):
    if hasattr(utils, name):
        warnings.warn(
            f"Importing '{name}' from 'sedona.utils' is deprecated. Please use 'sedona.spark.utils.{name}' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(utils, name)
    raise AttributeError(f"module 'sedona.utils' has no attribute '{name}'")


warnings.warn(
    "Importing from 'sedona.utils' is deprecated. Please use 'sedona.spark.utils' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = utils.__all__
