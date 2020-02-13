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
