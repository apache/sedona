# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from setuptools import setup, find_packages
from os import path
from sedona import version

here = path.abspath(path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='apache-sedona',
    version=version,
    description='Apache Sedona is a cluster computing system for processing large-scale spatial data',
    url='https://sedona.apache.org',
    license="Apache License v2.0",
    author='Apache Sedona',
    author_email='dev@sedona.apache.org',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.6',
    install_requires=['attrs', "shapely"],
    extras_require={"spark": ['pyspark>=2.3.0']},
    project_urls={
        'Documentation': 'https://sedona.apache.org',
        'Source code': 'https://github.com/apache/incubator-sedona',
        'Bug Reports': 'https://issues.apache.org/jira/projects/SEDONA'
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License"
    ]
)

