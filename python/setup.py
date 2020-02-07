from setuptools import setup, find_packages
from os import path
from geospark import version

here = path.abspath(path.dirname(__file__))
jars_relative_path = "geospark/jars"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='geospark',
    version=version,
    description='GeoSpark Python',
    url='https://github.com/DataSystemsLab/GeoSpark/tree/master/python',
    author='Pawel Kocinski',
    author_email='pawel93kocinski@gmail.com',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.6',
    install_requires=['pyspark', 'findspark', 'attrs', "shapely"],
    project_urls={
        'Bug Reports': 'https://github.com/DataSystemsLab/GeoSpark'
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License"
    ],
    package_data={
        'geospark.jars.2_3': ["*.jar"],
        'geospark.jars.2_4': ["*.jar"],
        'geospark.jars.2_2': ["*.jar"]
    }
)

