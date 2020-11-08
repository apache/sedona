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


@attr.s
class JvmPartitioner:
    jpart = attr.ib()

    def assignPartitionIds(self):
        raise NotImplementedError("Currently not supported")

    def assignPartitionLineage(self):
        raise NotImplementedError("Currently not supported")

    def dropElements(self):
        raise NotImplementedError("Currently not supported")

    def equals(self):
        raise NotImplementedError("Currently not supported")

    def findZone(self):
        raise NotImplementedError("Currently not supported")

    def forceGrowUp(self):
        raise NotImplementedError("Currently not supported")

    def getAllZones(self):
        raise NotImplementedError("Currently not supported")

    def getClass(self):
        raise NotImplementedError("Currently not supported")

    def getElements(self):
        raise NotImplementedError("Currently not supported")

    def getLeafZones(self):
        raise NotImplementedError("Currently not supported")

    def getParentZone(self):
        raise NotImplementedError("Currently not supported")

    def getTotalNumLeafNode(self):
        raise NotImplementedError("Currently not supported")

    def getZone(self):
        raise NotImplementedError("Currently not supported")

    def hashCode(self):
        raise NotImplementedError("Currently not supported")

    def insert(self):
        raise NotImplementedError("Currently not supported")

    def isLeaf(self):
        raise NotImplementedError("Currently not supported")

    def notify(self):
        raise NotImplementedError("Currently not supported")

    def notifyAll(self):
        raise NotImplementedError("Currently not supported")

    def toString(self):
        raise NotImplementedError("Currently not supported")

    def wait(self):
        raise NotImplementedError("Currently not supported")
