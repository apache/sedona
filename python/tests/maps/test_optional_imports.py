from tests.maps import uninstall_keplergl
from tests.maps import uninstall_pydeck

from tests.test_base import TestBase
from sedona.spark import *

# Try importing uninstalled modules to check if they are truly uninstalled
try:
    from keplergl import KeplerGl
except ModuleNotFoundError:
    keplerUninstalled = True

try:
    import pydeck as pdk
except ModuleNotFoundError:
    pydeckUninstalled = True




class TestOptionalImport(TestBase):


    def test_optional_import(self):

        ## If this test manages to run,that means sedona.spark import * succeeds even when kepler and pydeck are uninstalled.
        assert keplerUninstalled == True and pydeckUninstalled == True