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

# 发布 SNAPSHOT 版本

本步骤用于将 Maven SNAPSHOT 发布到 https://repository.apache.org

对于发布经理（release manager）来说，这是检验自己凭据配置的良好实践。

详细要求见 [ASF Infra 网站](https://infra.apache.org/publishing-maven-artifacts.html)。

!!!warning
    本页所有脚本都应在本地 Sedona Git 仓库的 master 分支下，通过单一脚本文件运行。

## 0. 准备一个空脚本文件

1. 在本地 Sedona Git 仓库 master 分支下运行：

```bash
echo '#!/bin/bash' > create-release.sh
chmod 777 create-release.sh
```

2. 用您喜欢的 GUI 文本编辑器打开 `create-release.sh`。
3. 然后不断把本页上的脚本复制粘贴到该文件中，覆盖原内容。
4. 不要直接将脚本复制粘贴到终端，因为 `clipboard.js` 的一个 bug 会在这种情形下产生换行符问题。
5. 每次更新该脚本后，运行 `./create-release.sh` 执行它。

## 1. 上传 snapshot 版本

在您的 Sedona GitHub 仓库中运行：

```bash
#!/bin/bash

git checkout master
git pull

rm -f release.*
rm -f pom.xml.*

# 校验 POM 与凭据配置
mvn -q -B clean release:prepare -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.current_snapshot }} -Dresume=false -DdryRun=true -Penable-all-submodules -Darguments="-DskipTests"
mvn -q -B release:clean -Penable-all-submodules

# Spark 3.3 与 Scala 2.12
mvn -q deploy -DskipTests -Dspark=3.3 -Dscala=2.12

# Spark 3.3 与 Scala 2.13
mvn -q deploy -DskipTests -Dspark=3.3 -Dscala=2.13

# Spark 3.4 与 Scala 2.12
mvn -q deploy -DskipTests -Dspark=3.4 -Dscala=2.12

# Spark 3.4 与 Scala 2.13
mvn -q deploy -DskipTests -Dspark=3.4 -Dscala=2.13
```
