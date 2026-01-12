import shapely


def has_sedona_serializer_speedup():
    try:
        from . import geomserde_speedup
    except ImportError:
        return False
    return True

def to_sedona_func(arr):
    try:
        from . import geomserde_speedup
    except ImportError:
        return shapely.to_wkb(arr)

    return geomserde_speedup.to_sedona_func(arr)


def from_sedona_func(arr):
    try:
        from . import geomserde_speedup
    except ImportError:
        return shapely.from_wkb(arr)

    return geomserde_speedup.from_sedona_func(arr)
