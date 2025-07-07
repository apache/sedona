<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Building the Documentation

This folder contains the Sphinx-based documentation for the Apache Sedona Python library. Follow the steps below to build the documentation locally.

## Prerequisites

Ensure you have the following installed:

- Python 3.6 or later

- `pip` (Python package manager)

- Sphinx and required extensions:

```bash
  pip install sphinx sphinx_rtd_theme
```

## Steps to Build the Documentation

- Navigate to the doc folder:

```bash
cd doc
```

- Clean previous builds: Run the following command to remove any previous build artifacts:

```bash
make clean
```

- Build the HTML documentation: Use the make command to generate the HTML documentation:

```bash
make html
```

- View the documentation: Open the generated HTML files located in the _build/html directory in your browser.
