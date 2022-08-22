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

from typing import Dict, Any, List


class SuiteContainer:

    def __init__(self, container: Dict[str, Any]):
        self.container = container

    @classmethod
    def empty(cls):
        return cls(container=dict(function_name=None, arguments=None, expected_result=None, transform=None))

    def with_function_name(self, function_name: str):
        self.container["function_name"] = function_name
        return self.__class__(
            container=self.container
        )

    def with_expected_result(self, expected_result: Any):
        self.container["expected_result"] = expected_result
        return self.__class__(
            container=self.container
        )

    def with_arguments(self, arguments: List[str]):
        self.container["arguments"] = arguments
        return self.__class__(
            container=self.container
        )

    def with_transform(self, transform: str):
        self.container["transform"] = transform
        return self.__class__(
            container=self.container
        )

    def __iter__(self):
        return self.container.__iter__()

    def __getitem__(self, name):
        return self.container.__getitem__(name)
