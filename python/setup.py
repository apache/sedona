from setuptools import setup
import numpy

setup(
    include_dirs=[numpy.get_include()],
)
