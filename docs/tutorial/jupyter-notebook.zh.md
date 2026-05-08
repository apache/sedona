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

# Python Jupyter Notebook 示例

Sedona Python 提供了一系列 [Jupyter Notebook 示例](https://github.com/apache/sedona/blob/master/docs/usecases/)。

请按以下步骤使用 Pipenv 在本机运行 Jupyter Notebook：

1. 克隆 Sedona GitHub 仓库或下载源代码
2. 从 PyPI 或 GitHub 源码安装 Sedona Python：参考 [安装 Sedona Python](../setup/install-python.md#install-sedona)。
3. 准备 spark-shaded jar：参考 [安装 Sedona Python](../setup/install-python.md#prepare-sedona-spark-jar)。
4. 设置 Pipenv 的 Python 版本（请使用您所需的 Python 版本）：

```bash
cd docs/usecases
pipenv --python 3.8
```

5. 安装依赖：

```bash
cd docs/usecases
pipenv install
```

6. 在 Pipenv 中安装 Jupyter Notebook 内核：

```bash
pipenv install ipykernel
pipenv shell
```

7. 在 Pipenv shell 中执行：

```bash
python -m ipykernel install --user --name=apache-sedona
```

8. 如果之前未配置过，请设置环境变量 `SPARK_HOME` 与 `PYTHONPATH`，参考 [安装 Sedona Python](../setup/install-python.md/#setup-environment-variables)。
9. 启动 Jupyter Notebook：`jupyter notebook`
10. 选择 Sedona notebook，在 notebook 中依次选择 Kernel -> Change Kernel，选择刚才注册的内核。
