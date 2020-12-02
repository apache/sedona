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

import attr

from sedona.core.jvm.abstract import JvmObject
from sedona.utils.decorators import require


class FileDataSplitter(Enum):
    CSV = "CSV"
    TSV = "TSV"
    GEOJSON = "GEOJSON"
    WKT = "WKT"
    WKB = "WKB"
    COMA = "COMA"
    TAB = "TAB"
    QUESTIONMARK = "QUESTIONMARK"
    SINGLEQUOTE = "SINGLEQUOTE"
    QUOTE = "QUOTE"
    UNDERSCORE = "UNDERSCORE"
    DASH = "DASH"
    PERCENT = "PERCENT"
    TILDE = "TILDE"
    PIPE = "PIPE"
    SEMICOLON = "SEMICOLON"


@attr.s
class FileSplitterJvm(JvmObject):
    splitter = attr.ib(type=FileDataSplitter)

    def _create_jvm_instance(self):
        return self.jvm_splitter(self.splitter.value) if self.splitter.value is not None else None

    @property
    @require(["FileDataSplitter"])
    def jvm_splitter(self):
        return self.jvm.FileDataSplitter.getFileDataSplitter
