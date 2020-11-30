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

class ImportedJvmLib:
    _imported_libs = []

    @classmethod
    def has_library(cls, library: str) -> bool:
        return library in cls._imported_libs

    @classmethod
    def import_lib(cls, library: str) -> bool:
        if library not in cls._imported_libs:
            cls._imported_libs.append(library)
        else:
            return False
        return True
