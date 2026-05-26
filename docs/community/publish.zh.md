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

# 发布 Sedona 版本

本页面面向 Sedona PMC，用于发布 Sedona 版本。

!!!warning
    本页所有脚本都应在本地 Sedona Git 仓库 master 分支下，通过单一脚本文件运行。

## 0. 准备一个空脚本文件

1. 在本地 Sedona Git 仓库 master 分支下运行：

```bash
echo '#!/bin/bash' > create-release.sh
chmod 777 create-release.sh
```

2. 用您喜欢的 GUI 文本编辑器打开 `create-release.sh`。
3. 不断把本页上的脚本复制粘贴到该文件中，覆盖原有内容。
4. 不要直接将脚本复制粘贴到终端，因为 `clipboard.js` 的一个 bug 会在这种情形下产生换行符问题。
5. 每次更新该脚本后运行 `./create-release.sh` 来执行。

## 1. 检查所有文件头中的 ASF 版权声明

1. 运行以下脚本：

```bash
#!/bin/bash
wget -q https://archive.apache.org/dist/creadur/apache-rat-0.15/apache-rat-0.15-bin.tar.gz
tar -xvf  apache-rat-0.15-bin.tar.gz
git clone --shared --branch master https://github.com/apache/sedona.git sedona-src
java -jar apache-rat-0.15/apache-rat-0.15.jar -d sedona-src > report.txt
```

2. 阅读生成的 report.txt 文件，确保所有源代码文件都包含 ASF 版权声明。
3. 删除生成的报告与克隆的源码：

```bash
#!/bin/bash
rm -rf apache-rat-0.15
rm -rf sedona-src
rm report.txt
```

## 2. 更新 Sedona Python、R 与 Zeppelin 的版本号

请确认以下文件中的 Sedona 版本均为 {{ sedona_create_release.current_version }}：

1. https://github.com/apache/sedona/blob/master/python/sedona/version.py
2. [python/pyproject.toml](https://github.com/apache/sedona/blob/master/python/pyproject.toml)（`[project]` 下的 `version` 字段）
3. https://github.com/apache/sedona/blob/master/R/DESCRIPTION
4. https://github.com/apache/sedona/blob/99239524f17389fc4ae9548ea88756f8ea538bb9/R/R/dependencies.R#L42
5. https://github.com/apache/sedona/blob/master/zeppelin/package.json

!!!warning
    `python/pyproject.toml` 中的 `version` 字段是 `setuptools` 在构建 Python sdist 与 wheel 时使用的版本号。如果该字段未更新，即便 `python/sedona/version.py` 已更新，发布到 PyPI 的 artifact 仍会带有旧版本号。

## 3. 更新发布说明

在 cut release candidate 之前，请确保 `docs/setup/release-notes.md` 已包含 {{ sedona_create_release.current_version }} 的条目，总结自上一版本以来的所有变更。该文件会被投票邮件与公告邮件直接引用，因此必须出现在发布 tag 中。

## 4. 更新 mkdocs.yml

* 把 `mkdocs.yml` 中的以下变量改为本次要发布的版本：
    * `sedona_create_release.current_version`
    * `sedona_create_release.current_rc`
    * `sedona_create_release.current_git_tag`
    * `sedona_create_release.current_snapshot`
* 然后通过 `mkdocs serve` 编译网站，本地浏览器中即可看到本页生成的脚本。
* 如有需要，也可以发布该网站，发布步骤见本文末尾。

## 5. 暂存并上传 release candidate

```bash
#!/bin/bash

git checkout master
git pull

rm -f release.*
rm -f pom.xml.*

echo "*****Step 1. Stage the Release Candidate to GitHub."

mvn -B clean release:prepare -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.current_snapshot }} -Dresume=false -Penable-all-submodules -Darguments="-DskipTests"
mvn -B release:clean -Penable-all-submodules

echo "*****Step 2: Upload the Release Candidate to https://repository.apache.org."

## 注意：这里使用 maven-release-plugin 2.3.2 而非更新的版本（如 3.0.1），是为了规避一个会阻止我们使用 -Dtag=<tag> clone Git 仓库的 bug。
## 详细信息见 https://issues.apache.org/jira/browse/MRELEASE-933 与 https://issues.apache.org/jira/browse/SCM-729。
##
## 同时请注意：系统属性 `-Dspark` 与 `-Dscala` 必须同时传给 release:perform 与 `-Darguments` 中的实际构建参数，
## 因为 release:perform 任务激活的 build profile 会同时影响实际构建任务。最稳妥的做法是两处都明确指定。

# 仓库信息
REPO_URL="https://github.com/apache/sedona.git"
RC_VERSION="{{ sedona_create_release.current_rc }}"
SEDONA_VERSION="{{ sedona_create_release.current_version }}"
TAG="sedona-${RC_VERSION}"
LOCAL_DIR="sedona-release"

# 如果已存在则先删除，然后克隆仓库
rm -rf $LOCAL_DIR && git clone --depth 1 --branch $TAG $REPO_URL $LOCAL_DIR && cd $LOCAL_DIR

# Maven release 插件版本
MAVEN_PLUGIN_VERSION="2.3.2"

# Spark 与 Scala 版本
declare -a SPARK_VERSIONS=("3.4" "3.5" "4.0" "4.1")
declare -a SCALA_VERSIONS=("2.12" "2.13")

# 根据 Spark 版本确定所需 Java 版本
get_java_version() {
  local spark_version=$1
  if [[ "$spark_version" == "4."* ]]; then
    echo "17"
  else
    echo "11"
  fi
}

# 查找 Maven 安装路径
find_maven_path() {
  # 通过多种方式定位 Maven
  local mvn_path=""

  # 方法 1：检查 mvn 是否在 PATH 中
  if command -v mvn >/dev/null 2>&1; then
    mvn_path=$(command -v mvn)
  fi

  # 方法 2：检查常见的 Homebrew 路径
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /opt/homebrew/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # 方法 3：检查 /usr/local（旧版本 Homebrew）
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /usr/local/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # 方法 4：检查系统路径
  if [[ -z "$mvn_path" ]]; then
    for path in /usr/bin/mvn /usr/local/bin/mvn; do
      if [[ -x "$path" ]]; then
        mvn_path="$path"
        break
      fi
    done
  fi

  if [[ -z "$mvn_path" ]]; then
    echo "ERROR: Could not find Maven installation" >&2
    echo "Please ensure Maven is installed and available in PATH or in standard locations" >&2
    exit 1
  fi

  echo "$mvn_path"
}

# 创建一个使用指定 Java 版本的 Maven 包装脚本
create_mvn_wrapper() {
  local java_version=$1
  local mvn_wrapper="/tmp/mvn-java${java_version}"
  local mvn_path=$(find_maven_path)

  echo "Using Maven at: $mvn_path" >&2

  # 创建包装脚本：先设置 JAVA_HOME 再执行 Maven
  cat > "$mvn_wrapper" << EOF
#!/bin/bash
JAVA_HOME="\${JAVA_HOME:-\$(/usr/libexec/java_home -v ${java_version})}" exec "${mvn_path}" "\$@"
EOF

  chmod +x "$mvn_wrapper"
  echo "$mvn_wrapper"
}

# 校验 Maven 包装脚本所用的 Java 版本
verify_java_version() {
  local mvn_wrapper=$1
  local expected_java_version=$2

  echo "Verifying Java version with Maven wrapper..."
  local mvn_java_version=$($mvn_wrapper --version | grep "Java version" | sed 's/.*Java version: \([0-9]*\).*/\1/')
  if [[ "$mvn_java_version" != "$expected_java_version" ]]; then
    echo "ERROR: Maven wrapper is using Java $mvn_java_version, but expected Java $expected_java_version"
    echo "Please ensure the correct Java version is installed"
    exit 1
  fi
  echo "✓ Verified: Maven wrapper is using Java $mvn_java_version"
}

# 遍历 Spark 与 Scala 版本组合
for SPARK in "${SPARK_VERSIONS[@]}"; do
  for SCALA in "${SCALA_VERSIONS[@]}"; do
    # Spark 4.0+ 与 Scala 2.12 不兼容，跳过
    if [[ "$SPARK" == "4."* && "$SCALA" == "2.12" ]]; then
      echo "Skipping Spark $SPARK with Scala $SCALA (not supported)"
      continue
    fi

    JAVA_VERSION=$(get_java_version $SPARK)
    echo "Running release:perform for Spark $SPARK and Scala $SCALA with Java $JAVA_VERSION..."

    # 用合适的 Java 版本创建 Maven 包装脚本
    MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
    echo "Created Maven wrapper: $MVN_WRAPPER"

    # 校验 Java 版本
    verify_java_version $MVN_WRAPPER $JAVA_VERSION

    # 通过包装脚本执行 Maven
    $MVN_WRAPPER org.apache.maven.plugins:maven-release-plugin:$MAVEN_PLUGIN_VERSION:perform \
      -DconnectionUrl=scm:git:file://$(pwd) \
      -Dtag=$TAG \
      -Dresume=false \
      -Darguments="-DskipTests -Dspark=$SPARK -Dscala=$SCALA" \
      -Dspark=$SPARK \
      -Dscala=$SCALA

    # 清理包装脚本
    rm -f $MVN_WRAPPER
  done
done

echo "*****Step 3: Upload Release Candidate on ASF SVN: https://dist.apache.org/repos/dist/dev/sedona"

echo "Creating ${RC_VERSION} folder on SVN..."

svn mkdir -m "Adding folder" https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}

echo "Creating release files locally..."

# 回到上层目录进行后续文件操作
cd ../..

echo "Downloading source code..."

wget https://github.com/apache/sedona/archive/refs/tags/sedona-${RC_VERSION}.tar.gz
tar -xvf sedona-${RC_VERSION}.tar.gz
mkdir apache-sedona-${SEDONA_VERSION}-src
cp -r sedona-sedona-${RC_VERSION}/* apache-sedona-${SEDONA_VERSION}-src/
tar czf apache-sedona-${SEDONA_VERSION}-src.tar.gz apache-sedona-${SEDONA_VERSION}-src
rm sedona-${RC_VERSION}.tar.gz
rm -rf sedona-sedona-${RC_VERSION}

# 为源码包生成校验和与签名
shasum -a 512 apache-sedona-${SEDONA_VERSION}-src.tar.gz > apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512
gpg -ab apache-sedona-${SEDONA_VERSION}-src.tar.gz

echo "Uploading source files..."

# 先上传源码相关文件
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-src.tar.gz https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-src.tar.gz
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-src.tar.gz.asc https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-src.tar.gz.asc
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512

echo "Compiling the source code..."

mkdir apache-sedona-${SEDONA_VERSION}-bin

# 根据 Spark 版本确定所需 Java 版本
get_java_version() {
  local spark_version=$1
  if [[ "$spark_version" == "4."* ]]; then
    echo "17"
  else
    echo "11"
  fi
}

# 查找 Maven 安装路径
find_maven_path() {
  # 通过多种方式定位 Maven
  local mvn_path=""

  # 方法 1：检查 mvn 是否在 PATH 中
  if command -v mvn >/dev/null 2>&1; then
    mvn_path=$(command -v mvn)
  fi

  # 方法 2：检查常见的 Homebrew 路径
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /opt/homebrew/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # 方法 3：检查 /usr/local（旧版本 Homebrew）
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /usr/local/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # 方法 4：检查系统路径
  if [[ -z "$mvn_path" ]]; then
    for path in /usr/bin/mvn /usr/local/bin/mvn; do
      if [[ -x "$path" ]]; then
        mvn_path="$path"
        break
      fi
    done
  fi

  if [[ -z "$mvn_path" ]]; then
    echo "ERROR: Could not find Maven installation" >&2
    echo "Please ensure Maven is installed and available in PATH or in standard locations" >&2
    exit 1
  fi

  echo "$mvn_path"
}

# 创建一个使用指定 Java 版本的 Maven 包装脚本
create_mvn_wrapper() {
  local java_version=$1
  local mvn_wrapper="/tmp/mvn-java${java_version}"
  local mvn_path=$(find_maven_path)

  echo "Using Maven at: $mvn_path" >&2

  # 创建包装脚本：先设置 JAVA_HOME 再执行 Maven
  cat > "$mvn_wrapper" << EOF
#!/bin/bash
JAVA_HOME="\${JAVA_HOME:-\$(/usr/libexec/java_home -v ${java_version})}" exec "${mvn_path}" "\$@"
EOF

  chmod +x "$mvn_wrapper"
  echo "$mvn_wrapper"
}

# 校验 Maven 包装脚本所用的 Java 版本
verify_java_version() {
  local mvn_wrapper=$1
  local expected_java_version=$2

  echo "Verifying Java version with Maven wrapper..."
  local mvn_java_version=$($mvn_wrapper --version | grep "Java version" | sed 's/.*Java version: \([0-9]*\).*/\1/')
  if [[ "$mvn_java_version" != "$expected_java_version" ]]; then
    echo "ERROR: Maven wrapper is using Java $mvn_java_version, but expected Java $expected_java_version"
    echo "Please ensure the correct Java version is installed"
    exit 1
  fi
  echo "✓ Verified: Maven wrapper is using Java $mvn_java_version"
}

# 使用 Java 11 编译 Spark 3.4
JAVA_VERSION=$(get_java_version "3.4")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 3.4 with Scala 2.12 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.4 -Dscala=2.12 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

echo "Compiling for Spark 3.4 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.4 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# 使用 Java 11 编译 Spark 3.5
JAVA_VERSION=$(get_java_version "3.5")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 3.5 with Scala 2.12 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.5 -Dscala=2.12 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

echo "Compiling for Spark 3.5 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.5 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# 使用 Java 17 编译 Spark 4.0
JAVA_VERSION=$(get_java_version "4.0")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 4.0 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=4.0 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# 使用 Java 17 编译 Spark 4.1
JAVA_VERSION=$(get_java_version "4.1")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 4.1 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=4.1 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# 清理 Maven 包装脚本
rm -f /tmp/mvn-java11 /tmp/mvn-java17

tar czf apache-sedona-${SEDONA_VERSION}-bin.tar.gz apache-sedona-${SEDONA_VERSION}-bin

# 为二进制包生成校验和与签名
shasum -a 512 apache-sedona-${SEDONA_VERSION}-bin.tar.gz > apache-sedona-${SEDONA_VERSION}-bin.tar.gz.sha512
gpg -ab apache-sedona-${SEDONA_VERSION}-bin.tar.gz

echo "Uploading binary files..."

# 上传二进制相关文件
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-bin.tar.gz https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-bin.tar.gz
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-bin.tar.gz.asc https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-bin.tar.gz.asc
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-bin.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-bin.tar.gz.sha512

echo "Removing local release files..."

rm apache-sedona-${SEDONA_VERSION}-src.tar.gz
rm apache-sedona-${SEDONA_VERSION}-src.tar.gz.asc
rm apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512
rm apache-sedona-${SEDONA_VERSION}-bin.tar.gz
rm apache-sedona-${SEDONA_VERSION}-bin.tar.gz.asc
rm apache-sedona-${SEDONA_VERSION}-bin.tar.gz.sha512
rm -rf apache-sedona-${SEDONA_VERSION}-src
rm -rf apache-sedona-${SEDONA_VERSION}-bin

```

## 6. 在 dev sedona.apache.org 邮件列表上投票

### 投票邮件

如有需要，请在邮件末尾追加变更说明：

```
Subject: [VOTE] Release Apache Sedona {{ sedona_create_release.current_rc }}

Hi all,

This is a call for vote on Apache Sedona {{ sedona_create_release.current_rc }}. Please refer to the changes listed at the bottom of this email.

Release notes:
https://github.com/apache/sedona/blob/{{ sedona_create_release.current_git_tag }}/docs/setup/release-notes.md

Build instructions:
https://github.com/apache/sedona/blob/{{ sedona_create_release.current_git_tag }}/docs/setup/compile.md

GitHub tag:
https://github.com/apache/sedona/releases/tag/{{ sedona_create_release.current_git_tag }}

GPG public key to verify the Release:
https://downloads.apache.org/sedona/KEYS

Source code and binaries:
https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/

The vote will be open for at least 72 hours or until at least 3 "+1" PMC votes are cast

Instruction for checking items on the checklist: https://sedona.apache.org/latest/community/vote/

We recommend you use this Jupyter notebook on MyBinder to perform this task: https://mybinder.org/v2/gh/jiayuasu/sedona-tools/HEAD?labpath=binder%2Fverify-release.ipynb

**Please vote accordingly and you must provide your checklist for your vote**.


[ ] +1 approve

[ ] +0 no opinion

[ ] -1 disapprove with the reason

Checklist:

[ ] Download links are valid.

[ ] Checksums and PGP signatures are valid.

[ ] Source code artifacts have correct names matching the current release.

For a detailed checklist  please refer to:
https://cwiki.apache.org/confluence/display/INCUBATOR/Incubator+Release+Checklist

------------

Changes according to the comments on the previous release
Original comment (Permalink from https://lists.apache.org/list.html):


```

### 投票通过邮件

请统计票数，并在邮件末尾加上投票邮件的 Permalink：

```
Subject: [RESULT][VOTE] Release Apache Sedona {{ sedona_create_release.current_rc }}

Dear all,

The vote closes now as 72hr have passed. The vote PASSES with

+? (binding): NAME1, NAME2, NAME3
+? (non-binding): NAME4
No -1 votes

The vote thread (Permalink from https://lists.apache.org/list.html):

I will make an announcement soon.

```

### 公告邮件

1. 该邮件应发送至 dev@sedona.apache.org。
2. 请加上投票邮件的 permalink。
3. 请加上投票结果邮件的 permalink。

```
Subject: [ANNOUNCE] Apache Sedona {{ sedona_create_release.current_version }} released

Dear all,

We are happy to report that we have released Apache Sedona {{ sedona_create_release.current_version }}. Thank you again for your help.

Apache Sedona is a cluster computing system for processing large-scale spatial data.


Vote thread (Permalink from https://lists.apache.org/list.html):


Vote result thread (Permalink from https://lists.apache.org/list.html):


Website:
https://sedona.apache.org/

Release notes:
https://github.com/apache/sedona/blob/sedona-{{ sedona_create_release.current_version }}/docs/setup/release-notes.md

Download links:
https://github.com/apache/sedona/releases/tag/sedona-{{ sedona_create_release.current_version }}

Additional resources:
Mailing list: dev@sedona.apache.org
Twitter: https://twitter.com/ApacheSedona
Gitter: https://gitter.im/apache/sedona

Regards,
Apache Sedona Team
```

## 7. 投票失败

如果投票未通过：

1. 在投票邮件中说明我们将创建另一个 release candidate。
2. 从第 4 步 `更新 mkdocs.yml` 重新开始。请把 release candidate ID 递增（如 `{{ sedona_create_release.current_version}}-rc2`），并相应更新 `mkdocs.yml` 中的 `sedona_create_release.current_rc` 与 `sedona_create_release.current_git_tag` 来生成本页脚本。

## 8. 发布源码与 Maven 包

### 上传发布版本

```bash
#!/bin/bash

echo "Move all files in https://dist.apache.org/repos/dist/dev/sedona to https://dist.apache.org/repos/dist/release/sedona, using svn"
svn mkdir -m "Adding folder" https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}
wget https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz
wget https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc
wget https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512
wget https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz
wget https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc
wget https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512 https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512 https://dist.apache.org/repos/dist/release/sedona/{{ sedona_create_release.current_version }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512
rm apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz
rm apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc
rm apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512
rm apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz
rm apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc
rm apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512
```

### 手动 close 并 release Maven 包

1. 在 https://repository.apache.org 的 `staging repository` 中找到 Sedona 暂存仓库，点击 `Close`。
2. staging repo close 完成后，再点击 `Release`。

## 9. 发布 Sedona Python

创建 GitHub release 后，Sedona GitHub CI 会自动把 wheel 文件发布到 PyPI。

## 10. 发布 Sedona Zeppelin

```bash
#!/bin/bash

wget https://github.com/apache/sedona/archive/refs/tags/{{ sedona_create_release.current_git_tag}}.tar.gz
tar -xvf {{ sedona_create_release.current_git_tag}}.tar.gz
mkdir apache-sedona-{{ sedona_create_release.current_version }}-src
cp -r sedona-{{ sedona_create_release.current_git_tag}}/* apache-sedona-{{ sedona_create_release.current_version }}-src/

rm -rf apache-{{ sedona_create_release.current_git_tag}}

cd apache-sedona-{{ sedona_create_release.current_version }}-src/zeppelin && npm publish && cd ..
rm -rf apache-sedona-{{ sedona_create_release.current_version }}-src
```

## 11. 将 Sedona R 发布到 CRAN

```bash
#!/bin/bash
R CMD build .
R CMD check --as-cran apache.sedona_*.tar.gz
```

然后通过 [此网页表单](https://xmpalantir.wu.ac.at/cransubmit/) 提交到 CRAN。

## 12. 发布文档网站

1. 在本地仓库中把 {{ sedona_create_release.current_version }} 的 Git tag check out 到名为 `branch-{{ sedona_create_release.current_version }}` 的分支。
2. 把下载链接添加到 [Download 页面](../download.md)。
3. 把新闻添加到 `docs/index.md`。
4. 将该分支的更改推送到 GitHub。
5. GitHub CI 会自动捕捉这些更改并部署到 `website` 分支。
6. 通常 [此仓库](https://github.com/jiayuasu/sedona-sync-action) 会按日自动发布该网站。
