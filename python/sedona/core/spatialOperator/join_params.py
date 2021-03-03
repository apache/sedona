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

import attr

from sedona.core.enums import IndexType
from sedona.core.enums.join_build_side import JoinBuildSide
from sedona.core.jvm.abstract import JvmObject


@attr.s
class JoinParams:
    useIndex = attr.ib(type=bool, default=True)
    considerBoundaryIntersection = attr.ib(type=bool, default=False)
    indexType = attr.ib(type=str, default=IndexType.RTREE)
    joinBuildSide = attr.ib(type=str, default=JoinBuildSide.LEFT)

    def jvm_instance(self, jvm):
        return JvmJoinParams(jvm, self.useIndex, self.considerBoundaryIntersection, self.indexType, self.joinBuildSide).jvm_instance


@attr.s
class JvmJoinParams(JvmObject):
    useIndex = attr.ib(type=bool, default=True)
    considerBoundaryIntersection = attr.ib(type=bool, default=False)
    indexType = attr.ib(type=str, default=IndexType.RTREE)
    joinBuildSide = attr.ib(type=str, default=JoinBuildSide.LEFT)

    def _create_jvm_instance(self):
        return self.jvm_reference(self.useIndex, self.considerBoundaryIntersection, self.indexType.value, self.joinBuildSide)

    @property
    def jvm_reference(self):
        return self.jvm.JoinParamsAdapter.createJoinParams
