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

# 编译 Sedona 源码

## 编译 Scala / Java 源码

Sedona Scala/Java 代码是一个多模块项目，每个模块都是 Scala/Java 混合工程，由 Apache Maven 3 管理。

* 请确保您的 Linux/Mac 机器上已安装 Java 11/17、Apache Maven 3.3.1+ 与 Python 3.8+。Sedona 的编译尚未在 Windows 机器上测试。

要编译所有模块，请确保位于所有模块的根目录，然后在终端运行以下命令：

=== "不运行单元测试"
	```bash
	mvn clean install -DskipTests
	```
	该命令会先删除旧的二进制文件，并编译所有模块；编译过程会跳过单元测试。如果只编译单个模块，请进入对应模块的目录后运行同一命令。

=== "运行单元测试"
	```bash
	mvn clean install
	```
	所有模块的 Maven 单元测试可能耗时长达 30 分钟。

=== "打包 Geotools jar"
	```bash
	mvn clean install -DskipTests -Dgeotools
	```
	Geotools 相关 jar 会被打入生成的 fat jar 中。

!!!note
	默认情况下，该命令会针对 Spark 3.4 和 Scala 2.12 编译 Sedona。

### 针对不同目标编译

可使用 `-Dspark` 与 `-Dscala` 命令行参数指定不同的目标：

* `-Dspark`：`{major}.{minor}`，例如 `-Dspark=3.4` 表示针对 Spark 3.4 编译。
* `-Dscala`：`2.12` 或 `2.13`

=== "Spark 3.4+ Scala 2.12"
	```
	mvn clean install -DskipTests -Dspark=3.4 -Dscala=2.12
	```
    若要针对更高 Spark 版本编译，请将 `3.4` 替换为对应的 Spark major.minor 版本。
=== "Spark 3.4+ Scala 2.13"
	```
	mvn clean install -DskipTests -Dspark=3.4 -Dscala=2.13
	```
    若要针对更高 Spark 版本编译，请将 `3.4` 替换为对应的 Spark major.minor 版本。

!!!tip
	如需获取打包好所有 GeoTools jar 的 Sedona Spark Shaded jar，只需追加 `-Dgeotools` 选项，例如：`mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.4 -Dgeotools`

### 下载 staged jar

Sedona 通过 GitHub Actions 在每次提交时自动生成 jar。您可以在 [此处](https://github.com/apache/sedona/actions/workflows/java.yml) 点击对应提交的 ==Artifacts== 标签下载这些 jar。

## 运行 Python 测试

1) 准备 Spark（如未安装则下载）并设置环境变量

```bash
export SPARK_VERSION=3.4.0   # 或其他受支持的版本
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz
rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
export SPARK_HOME=$PWD/spark-${SPARK_VERSION}-bin-hadoop3
export PYTHONPATH=$SPARK_HOME/python
```

2) 将所需的 JAI jar 放入 $SPARK_HOME/jars

```bash
export JAI_CORE_VERSION="1.1.3"
export JAI_CODEC_VERSION="1.1.3"
export JAI_IMAGEIO_VERSION="1.1"
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_core/${JAI_CORE_VERSION}/jai_core-${JAI_CORE_VERSION}.jar
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_codec/${JAI_CODEC_VERSION}/jai_codec-${JAI_CODEC_VERSION}.jar
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_imageio/${JAI_IMAGEIO_VERSION}/jai_imageio-${JAI_IMAGEIO_VERSION}.jar
```

3) 构建包含 GeoTools shaded 的 Sedona Scala/Java jar（从仓库根目录执行）

```bash
mvn clean install -DskipTests -Dgeotools
cp spark-shaded/target/sedona-spark-shaded-*.jar $SPARK_HOME/jars/
```

4) 准备 Python 开发环境

Python 包使用 `pyproject.toml`（PEP 517/518）并以 setuptools 作为构建后端。推荐使用 [uv](https://docs.astral.sh/uv/) 管理虚拟环境与依赖。

```bash
cd python
python -m pip install --upgrade uv
uv venv --python 3.10   # 或其他受支持的版本（>=3.8）
```

5) 安装匹配的 PySpark 版本与其他依赖

```bash
cd python
# 使用正确的 PySpark 版本，否则将默认安装最新版
uv add pyspark==${SPARK_VERSION} --optional spark
uv sync
```

6) 以可编辑模式安装 Sedona 并运行 Python 测试

```bash
cd python
uv pip install -e .
uv run pytest -v tests
```

## 编译文档

文档网站会在每次提交后自动构建，构建产物可在以下位置下载：

### MkDocs 网站

文档网站源码以 Markdown 编写，由 MkDocs 编译，主题基于 [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)。

在 Sedona 仓库中，MkDocs 配置文件 ==mkdocs.yml== 位于根目录，所有文档源码位于 `docs` 目录。

如需在本地编译源码并测试网站，请阅读 [MkDocs 教程](http://www.mkdocs.org/#installation) 与 [Materials for MkDocs 教程](https://squidfunk.github.io/mkdocs-material/getting-started/)。

简而言之，需要运行：

```
python3 -m pip install uv
uv sync --group docs
```

安装好 MkDocs 与 MkDocs-Material 后，在 Sedona 根目录运行：

```
uv run mkdocs build
uv run mike deploy --update-aliases latest-snapshot -b website -p
uv run mike serve
```

## pre-commit

我们通过 GitHub Actions 运行 [pre-commit](https://pre-commit.com/)，因此本地安装目前是可选的。

pre-commit 的[配置文件](https://github.com/apache/sedona/blob/master/.pre-commit-config.yaml)位于仓库根目录。在运行钩子之前，需要先安装 pre-commit。

钩子会在执行 `git commit` 时运行，也可以通过命令行 `pre-commit` 触发。部分钩子会在失败时自动修复代码，但大多数会输出 linter 错误信息。如果某个钩子失败，整个 commit 会失败，您需要修复问题后重新 `git add` 并提交。`git commit` 时钩子大多只针对修改过的文件运行；如果想对所有文件运行所有钩子，或者您正在新增钩子，应运行：

`pre-commit run --all-files`

有时您可能需要跳过某个钩子，例如该钩子阻碍了提交，或您本地缺少该钩子所需的依赖。`SKIP` 变量以英文逗号分隔多个钩子：

`SKIP=codespell git commit -m "foo"`

在直接运行 pre-commit 时同样适用：

`SKIP=codespell pre-commit run --all-files`

偶尔您在执行 `git commit` 时遇到更严重的问题，可以使用 `--no-verify` 跳过 pre-commit 检查直接提交，例如：

`git commit --no-verify -m "foo"`

如果只想运行某个钩子（例如 `markdownlint`），可使用：

`pre-commit run markdownlint --all-files`

仓库根目录下的 [Makefile](https://github.com/apache/sedona/blob/master/Makefile) 提供了三个与 pre-commit 相关的便捷命令。

例如，可以运行以下命令将 pre-commit 安装为每次提交前自动运行的钩子：

```
make checkinstall
```
