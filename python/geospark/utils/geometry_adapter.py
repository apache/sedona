from shapely.geometry.base import BaseGeometry

from geospark.core.geom.envelope import Envelope
from geospark.sql.types import GeometryFactory


class GeometryAdapter:

    @classmethod
    def create_jvm_geometry_from_base_geometry(cls, jvm, geom: BaseGeometry):
        """
        :param jvm:
        :param geom:
        :return:
        """
        if isinstance(geom, Envelope):
            jvm_geom = geom.create_jvm_instance(jvm)
        else:
            decoded_geom = GeometryFactory.to_bytes(geom)
            jvm_geom = jvm.GeometryAdapter.deserializeToGeometry(decoded_geom)

        return jvm_geom
