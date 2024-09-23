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
import inspect
import sys

# These allow use to access the __all__
import sedona.sql.st_aggregates as st_aggregates
import sedona.sql.st_constructors as st_constructors
import sedona.sql.st_functions as st_functions
import sedona.sql.st_predicates as st_predicates

# These bring the contents of the modules into this module
from sedona.sql.st_aggregates import *
from sedona.sql.st_constructors import *
from sedona.sql.st_functions import *
from sedona.sql.st_predicates import *

__all__ = (
    [
        name for name, obj in inspect.getmembers(sys.modules[__name__])
    ]  # get expected values from the modules
    + st_predicates.__all__
    + st_constructors.__all__
    + st_functions.__all__
    + st_aggregates.__all__
)
