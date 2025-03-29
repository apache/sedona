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

# Python Jupyter Notebook Examples

Sedona Python provides a number of [Jupyter Notebook examples](https://github.com/apache/sedona/blob/master/docs/usecases/).

Please use the following steps to run Jupyter notebook with Pipenv on your machine

1. Clone Sedona GitHub repo or download the source code
2. Install Sedona Python from PyPI or GitHub source: Read [Install Sedona Python](../setup/install-python.md#install-sedona) to learn.
3. Prepare spark-shaded jar: Read [Install Sedona Python](../setup/install-python.md#prepare-sedona-spark-jar) to learn.
4. Setup pipenv python version. Please use your desired Python version.

```bash
cd docs/usecases
pipenv --python 3.8
```

5. Install dependencies

```bash
cd docs/usecases
pipenv install
```

6. Install jupyter notebook kernel for pipenv

```bash
pipenv install ipykernel
pipenv shell
```

7. In the pipenv shell, do

```bash
python -m ipykernel install --user --name=apache-sedona
```

8. Setup environment variables `SPARK_HOME` and `PYTHONPATH` if you didn't do it before. Read [Install Sedona Python](../setup/install-python.md/#setup-environment-variables) to learn.
9. Launch jupyter notebook: `jupyter notebook`
10. Select Sedona notebook. In your notebook, Kernel -> Change Kernel. Your kernel should now be an option.
