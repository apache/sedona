from setuptools import setup, find_packages
from os import path
from sedona import version

here = path.abspath(path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='sedona',
    version=version,
    description='Apache Sedona is a cluster computing system for processing large-scale spatial data. Apache Sedona is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.',
    url='https://github.com/apache/incubator-sedona',
    author='Apache Sedona',
    author_email='dev@sedona.apache.org',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.6',
    install_requires=['pyspark<3.1.0', 'attrs', "shapely"],
    project_urls={
        'Bug Reports': 'https://github.com/apache/incubator-sedona'
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License"
    ]
)

