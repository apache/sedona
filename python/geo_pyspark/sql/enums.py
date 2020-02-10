from enum import Enum


class GeomEnum(Enum):
    undefined = 0
    point = 1
    polyline = 3
    polygon = 5
    multipoint = 8

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def get_name(cls, value):
        return cls._value2member_map_[value].name


class ShapeEnum(Enum):

    shape = 0
    circle = 1

    @classmethod
    def get_name(cls, value):
        return cls._value2member_map_[value].name
