from keplergl import KeplerGl
from sedona.maps.SedonaKepler import SedonaKepler
from tests.test_base import TestBase


class TestVisualization(TestBase):

    def test_map_creation(self):
        sedona_kepler_map = SedonaKepler.createMap()
        kepler_map = KeplerGl()
        assert sedona_kepler_map.config == kepler_map.config
