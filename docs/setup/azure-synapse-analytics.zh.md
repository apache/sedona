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

本教程指引您在启用了数据外泄保护（Data Exfiltration Protection，DEP）或由于其他网络限制导致 Spark 池无法访问公网的情况下，在 Azure Synapse Analytics 中安装 Sedona。

## 开始之前

本教程主要演示如何在 Spark 3.4、Python 3.10 上跑通 Sedona 1.6.1。

如果您希望运行更新的版本，请参考本文后半部分介绍的详细构建与诊断流程。

## 强烈建议

1. 从一个未安装其他包的干净 Spark 池开始，以避免依赖冲突。
2. Apache Spark 池 -> Apache Spark 配置：使用默认配置。

## Sedona 1.6.1 在 Spark 3.4、Python 3.10 上的安装

### 步骤 1：下载 9 个包

注意：版本必须严格匹配，最新并不总是最优。

来自 Maven：

- [sedona-spark-shaded-3.4_2.12-1.6.1.jar](https://mvnrepository.com/artifact/org.apache.sedona/sedona-spark-shaded-3.4_2.12/1.6.1)

- [geotools-wrapper-1.6.1-28.2.jar](https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper/1.6.1-28.2)

来自 PyPI：

- [rasterio-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/cd/ad/2d3a14e5a97ca827a38d4963b86071267a6cd09d45065cd753d7325699b6/rasterio-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

- [shapely-2.0.6-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/2b/a6/302e0d9c210ccf4d1ffadf7ab941797d3255dcd5f93daa73aaf116a4db39/shapely-2.0.6-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

- [apache_sedona-1.6.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/b6/71/09f7ca2b6697b2699c04d1649bb379182076d263a9849de81295d253220d/apache_sedona-1.6.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

- [click_plugins-1.1.1-py2.py3-none-any.whl](https://files.pythonhosted.org/packages/e9/da/824b92d9942f4e472702488857914bdd50f73021efea15b4cad9aca8ecef/click_plugins-1.1.1-py2.py3-none-any.whl)

- [cligj-0.7.2-py3-none-any.whl](https://files.pythonhosted.org/packages/73/86/43fa9f15c5b9fb6e82620428827cd3c284aa933431405d1bcf5231ae3d3e/cligj-0.7.2-py3-none-any.whl)

- [affine-2.4.0-py3-none-any.whl](https://files.pythonhosted.org/packages/0b/f7/85273299ab57117850cc0a936c64151171fac4da49bc6fba0dad984a7c5f/affine-2.4.0-py3-none-any.whl)

- [numpy-2.1.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/fb/25/ba023652a39a2c127200e85aed975fc6119b421e2c348e5d0171e2046edb/numpy-2.1.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

### 步骤 2：将包上传到 Synapse 工作区

https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-manage-workspace-packages

### 步骤 3：将包添加到 Spark 池

本教程采用该页面中的第二种方式：**If you are updating from the Synapse Studio**

https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-manage-pool-packages#manage-packages-from-synapse-studio-or-azure-portal

### 步骤 4：notebook

在 notebook 起始处加入以下代码：

```python
from sedona.spark import SedonaContext

config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.4_2.12-1.6.1,"
        "org.datasyslab:geotools-wrapper-1.6.1-28.2",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions",
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)
```

跑一个测试：

```python
sedona.sql("SELECT ST_GeomFromEWKT('SRID=4269;POINT(40.7128 -74.0060)')").show()
```

如果看到点的输出，则说明安装成功，配置工作至此完成。

## Sedona 1.6.0 在 Spark 3.4、Python 3.10 上需要的包

```
spark-xml_2.12-0.17.0.jar
sedona-spark-shaded-3.4_2.12-1.6.0.jar

click_plugins-1.1.1-py2.py3-none-any.whl
affine-2.4.0-py3-none-any.whl
apache_sedona-1.6.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
cligj-0.7.2-py3-none-any.whl
rasterio-1.3.10-cp310-cp310-manylinux2014_x86_64.whl
shapely-2.0.4-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
snuggs-1.4.7-py3-none-any.whl
geotools-wrapper-1.6.0-28.2.jar
```

## 背景：如何为其他/未来版本的 Spark 与/或 Sedona 确定所需包

警告：该流程需要相当的技术耐心与排错能力。

整体步骤：用与已部署 Spark 池相同的镜像构建一台 Linux 虚拟机，按 Synapse 进行配置，安装 Sedona 包，再识别出在 Synapse 基线之上额外需要的包。

下面是 Sedona 1.6.1 在 Spark 3.4、Python 3.10 上的实操过程（同一流程也用于 Sedona 1.6.0）。

### 步骤 1：根据版本识别 Spark 池所用的 Linux 镜像

https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-34-runtime

### 步骤 2：下载 ISO

https://github.com/microsoft/azurelinux/tree/2.0

### 步骤 3：构建虚拟机

https://github.com/microsoft/azurelinux/blob/2.0/toolkit/docs/quick_start/quickstart.md#iso-image

如使用 Hyper-V，需注意以下设置：

- 启用 Secure Boot：Microsoft UEFI Certificate authority
- CPU 核心数：2
- 关闭动态内存（固定为 8GB）。忘记此设置会带来很多麻烦。

### 步骤 4：更新虚拟机

连接到虚拟机，注意首次启动会比预期更慢。

```sh
sudo dnf upgrade
```

### 步骤 5：可选但强烈推荐 —— 安装 ssh-server（以便复制粘贴）

```sh
sudo tdnf install -y openssh-server
```

启用 root 与密码认证：

```sh
sudo vi /etc/ssh/sshd_config
-	PasswordAuthentication yes
-	PermitRootLogin yes
```

启动 ssh-server：

```bash
sudo systemctl enable --now sshd.service
```

识别虚拟机 IP（这里在 Windows 10 桌面上使用 Hyper-V）：

```ps
Get-VMNetworkAdapter -VMName "Synapse Spark 3.4 Python 3.10 Sedona 1.6.1" | Select-Object -ExpandProperty IPAddresses
```

### 步骤 6：安装 Miniconda

```bash
cd /tmp
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
```

### 步骤 7：安装编译器

```sh
sudo tdnf -y install gcc g++
```

### 步骤 8：创建 Synapse 基线虚拟环境

下载虚拟环境定义文件：

```bash
wget -O Synapse-Python310-CPU.yml https://raw.githubusercontent.com/microsoft/synapse-spark-runtime/refs/heads/main/Synapse/spark3.4/Synapse-Python310-CPU.yml source
```

```bash
conda env create -f Synapse-Python310-CPU.yml -n synapse
```

如果在 `fsspec_wrapper` 处报错，请从 yml 中移除 `fsspec_wrapper==0.1.13=py_3` 后重试。

如果在上述修改之后仍出现来自 `pip` 的其他错误，可以忽略，仍可继续。

### 步骤 9：安装 Sedona Python 包

```bash
conda activate synapse
echo "apache-sedona==1.6.1" > requirements.txt
pip install -r requirements.txt > pip-output.txt
```

### 步骤 10：识别需要下载的 Python 包

```bash
grep Downloading pip-output.txt
```

**该输出即为您需要从 PyPI 定位并下载的包列表。**

输出示例：

```
Downloading apache_sedona-1.6.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (177 kB)
Downloading shapely-2.0.6-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (2.5 MB)
Downloading rasterio-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (22.2 MB)
Downloading affine-2.4.0-py3-none-any.whl (15 kB)
Downloading cligj-0.7.2-py3-none-any.whl (7.1 kB)
Downloading click_plugins-1.1.1-py2.py3-none-any.whl (7.5 kB)
```

### 步骤 11：在已部署的 Azure Synapse Spark 池（真实环境，不是虚拟机）中识别包冲突

- 将包上传到工作区
- 将包加入您的（干净的！）Spark 池

请仔细查看 Synapse 返回的错误信息，并逐一排查冲突。

注意：在 Spark 3.4 上安装 Sedona 1.6.0 时未遇到问题，但 Sedona 1.6.1 及其支持包出现了与 `numpy` 相关的冲突，需要下载特定版本并加入包列表。`numpy` 并未出现在前面 grep 的输出中。
