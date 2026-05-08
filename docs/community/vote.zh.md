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

# 投票发布 Sedona 版本

本页面面向 Sedona 社区，用于对 Sedona 发布版本进行投票。下面的脚本已在 macOS 上测试通过。

要对 Sedona 发布版本投票，您需要在投票邮件中提供至少包含以下条目的检查清单：

* 下载链接有效
* 校验和与 PGP 签名有效
* 已包含 DISCLAIMER 与 NOTICE
* 源码 artifact 名称与当前发布版本一致
* 项目能从源码成功编译

为方便您完成校验，我们提供了一份基于 MyBinder 的 [在线 Jupyter Notebook](https://github.com/jiayuasu/sedona-tools)。点击下方按钮即可打开 notebook 进行校验：[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jiayuasu/sedona-tools/HEAD?labpath=binder%2Fverify-release.ipynb)。校验通过后，即可在投票邮件中投 `+1`。

如果您更愿意在本机执行这些步骤，请阅读下方的步骤说明。如果能成功完成所有步骤，则上述检查清单都会得到满足，您即可在投票邮件中投 `+1` 并附上检查清单。

## 安装必要软件

1. GPG：在 Mac 上 `brew install gnupg gnupg2`。可在终端中通过 `gpg --version` 检查。
2. JDK 1.8 或 1.11。Mac 上可能安装了多种 Java 版本，可以尝试当前版本，但不保证一定能通过。可在终端通过 `java --version` 检查。
3. Apache Maven 3.3.1+。在 Mac 上 `brew install maven`。可在终端通过 `mvn -version` 检查。
4. Python3：macOS 默认带有 Python3。可在终端通过 `python3 --version` 检查。

如果您之前已安装过这些软件，可跳过本步骤。

## 运行校验脚本

请将 SEDONA\_CURRENT\_RC 与 SEDONA\_CURRENT\_VERSION 替换为正确的版本号，然后把内容粘贴到名为 `verify.sh` 的脚本中，并将输出重定向到文件。运行方式：

```bash
#!/bin/bash

## 修改脚本权限为可执行
chmod 777 verify.sh

## 运行并把输出重定向到文件
./verify.sh &> verify.out
```

`verify.sh` 内容如下。==如果直接复制下方内容，长行可能会被自动加上换行，请在本地脚本中将其去掉。==

```bash
#!/bin/bash

SEDONA_CURRENT_RC={{ sedona_create_release.current_rc }}
SEDONA_CURRENT_VERSION={{ sedona_create_release.current_version }}

## 下载 Sedona 发布
wget -q https://downloads.apache.org/sedona/KEYS
wget -q https://dist.apache.org/repos/dist/dev/sedona/$SEDONA_CURRENT_RC/apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz
wget -q https://dist.apache.org/repos/dist/dev/sedona/$SEDONA_CURRENT_RC/apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz.asc
wget -q https://dist.apache.org/repos/dist/dev/sedona/$SEDONA_CURRENT_RC/apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz.sha512
wget -q https://dist.apache.org/repos/dist/dev/sedona/$SEDONA_CURRENT_RC/apache-sedona-$SEDONA_CURRENT_VERSION-bin.tar.gz
wget -q https://dist.apache.org/repos/dist/dev/sedona/$SEDONA_CURRENT_RC/apache-sedona-$SEDONA_CURRENT_VERSION-bin.tar.gz.asc
wget -q https://dist.apache.org/repos/dist/dev/sedona/$SEDONA_CURRENT_RC/apache-sedona-$SEDONA_CURRENT_VERSION-bin.tar.gz.sha512

## 校验签名与校验和
gpg --import KEYS
gpg --verify apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz.asc
gpg --verify apache-sedona-$SEDONA_CURRENT_VERSION-bin.tar.gz.asc
shasum -a 512 apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz
cat apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz.sha512
shasum -a 512 apache-sedona-$SEDONA_CURRENT_VERSION-bin.tar.gz
cat apache-sedona-$SEDONA_CURRENT_VERSION-bin.tar.gz.sha512

## 解压源码包
tar -xvf apache-sedona-$SEDONA_CURRENT_VERSION-src.tar.gz

## 从源码编译项目
(cd apache-sedona-$SEDONA_CURRENT_VERSION-src;mvn clean install -DskipTests)

```

* 如果运行成功，输出文件中应能看到与下面类似的信息，并包含 `Good signature from`，最后 4 行应是两对相互匹配的校验和：

```
gpg: key 3A79A47AC26FF4CD: "Jia Yu <jiayu@apache.org>" not changed
gpg: key 6C883CA80E7FD299: "PawelKocinski <imbruced@apache.org>" not changed
gpg: Total number processed: 2
gpg:              unchanged: 2
gpg: assuming signed data in 'apache-sedona-1.2.0-incubating-src.tar.gz'
gpg: Signature made Mon Apr  4 11:48:31 2022 PDT
gpg:                using RSA key 949DD6275C69AB954B1872FC6C883CA80E7FD299
gpg:                issuer "imbruced@apache.org"
gpg: Good signature from "PawelKocinski <imbruced@apache.org>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 949D D627 5C69 AB95 4B18  72FC 6C88 3CA8 0E7F D299
gpg: assuming signed data in 'apache-sedona-1.2.0-incubating-bin.tar.gz'
gpg: Signature made Mon Apr  4 11:48:42 2022 PDT
gpg:                using RSA key 949DD6275C69AB954B1872FC6C883CA80E7FD299
gpg:                issuer "imbruced@apache.org"
gpg: Good signature from "PawelKocinski <imbruced@apache.org>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 949D D627 5C69 AB95 4B18  72FC 6C88 3CA8 0E7F D299
d3bdfd4d870838ebe63f21cb93634d2421ec1ac1b8184636206a5dc0d89a78a88257798b1f17371ad3cfcc3b1eb79c69e1410afdefeb4d9b52fc8bb5ea18dd2e  apache-sedona-1.2.0-incubating-src.tar.gz
d3bdfd4d870838ebe63f21cb93634d2421ec1ac1b8184636206a5dc0d89a78a88257798b1f17371ad3cfcc3b1eb79c69e1410afdefeb4d9b52fc8bb5ea18dd2e  apache-sedona-1.2.0-incubating-src.tar.gz
64cea38dd3ca171ee4e2a7365dbce999773862f2a11599bd0f27e9551d740659a519a9b976b3e7b0826088010967093e6acc9462f7073e9737c24b007a2df846  apache-sedona-1.2.0-incubating-bin.tar.gz
64cea38dd3ca171ee4e2a7365dbce999773862f2a11599bd0f27e9551d740659a519a9b976b3e7b0826088010967093e6acc9462f7073e9737c24b007a2df846  apache-sedona-1.2.0-incubating-bin.tar.gz
```

* 如果源码编译成功，您还会在输出末尾看到 `BUILD SUCCESS`。如果该步骤失败，可联系 Sedona PMC 询问是否仅是您本地环境的问题。

## 手动检查文件

1. 检查下载的文件版本号是否正确。

2. 解压源码目录后，检查 DISCLAIMER 与 NOTICE 文件是否存在且为最新。
