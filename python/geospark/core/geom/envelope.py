from shapely.geometry import Polygon, Point
from shapely.geometry.base import BaseGeometry

from geospark.utils.decorators import require


class Envelope(Polygon):

    def __init__(self, minx=0, maxx=1, miny=0, maxy=1):
        self.minx = minx
        self.maxx = maxx
        self.miny = miny
        self.maxy = maxy
        super().__init__([
            [self.minx, self.miny],
            [self.minx, self.maxy],
            [self.maxx, self.maxy],
            [self.maxx, self.miny]
        ])

    @require(["Envelope"])
    def create_jvm_instance(self, jvm):
        return jvm.Envelope(
            self.minx, self.maxx, self.miny, self.maxy
        )

    @classmethod
    def from_jvm_instance(cls, java_obj):
        return cls(
            minx=java_obj.getMinX(),
            maxx=java_obj.getMaxX(),
            miny=java_obj.getMinY(),
            maxy=java_obj.getMaxY(),
        )

    def to_bytes(self):
        from geospark.utils.binary_parser import BinaryBuffer
        bin_buffer = BinaryBuffer()
        bin_buffer.put_double(self.minx)
        bin_buffer.put_double(self.maxx)
        bin_buffer.put_double(self.miny)
        bin_buffer.put_double(self.maxy)
        return bin_buffer.byte_array

    @classmethod
    def from_shapely_geom(cls, geometry: BaseGeometry):
        if isinstance(geometry, Point):
            return cls(geometry.x, geometry.x, geometry.y, geometry.y)
        else:
            envelope = geometry.envelope
            exteriors = envelope.exterior
            coordinates = list(exteriors.coords)
            x_coord = [coord[0] for coord in coordinates]
            y_coord = [coord[1] for coord in coordinates]

        return cls(min(x_coord), max(x_coord), min(y_coord), max(y_coord))

    def __reduce__(self):
        return (self.__class__, (), dict(
            minx=self.minx,
            maxx=self.maxx,
            miny=self.miny,
            maxy=self.maxy,

        ))

    def __getstate__(self):
        return dict(
            minx=self.minx,
            maxx=self.maxx,
            miny=self.miny,
            maxy=self.maxy,

        )

    def __setstate__(self, state):
        self.minx = state.get("minx", 0)
        self.minx = state.get("maxx", 1)
        self.minx = state.get("miny", 0)
        self.minx = state.get("maxy", 1)

    @property
    def __array_interface__(self):
        raise NotImplementedError()

    def _get_coords(self):
        raise NotImplementedError()

    def _set_coords(self, ob):
        raise NotImplementedError()

    @property
    def coords(self):
        raise NotImplementedError()