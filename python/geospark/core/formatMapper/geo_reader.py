import abc

import attr

from geospark.utils.meta import MultipleMeta


@attr.s
class GeoDataReader(metaclass=MultipleMeta):

    @abc.abstractmethod
    def validate_imports(self):
        pass

    @abc.abstractmethod
    def readToGeometryRDD(cls, *args, **kwargs):
        raise NotImplementedError(f"Instance of the class {cls.__class__.__name__} has to implement method readToGeometryRDD")