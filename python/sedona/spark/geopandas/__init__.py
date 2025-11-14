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

"""
.. versionadded:: 1.8.0
    geopandas API on Sedona
"""

from sedona.spark.geopandas.geoseries import GeoSeries
from sedona.spark.geopandas.geodataframe import GeoDataFrame

from sedona.spark.geopandas.tools import sjoin

from sedona.spark.geopandas.io import read_file, read_parquet

# This used to default to False, but Spark 4.0.0 changed it to True
# We also want also it to default to True for Sedona, so we set it here
# to apply the change for users using Spark < 4.0.0.

# Having this set to False will cause these errors, which most users should not have to worry about:
# ValueError: Cannot combine the series or dataframe because it comes from a different dataframe.
# In order to allow this operation, enable 'compute.ops_on_diff_frames' option.
import pyspark.pandas as ps

ps.set_option("compute.ops_on_diff_frames", True)
