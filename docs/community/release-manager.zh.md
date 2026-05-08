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

# 成为发布经理

只有第一次担任发布经理时，才需要执行下面这些步骤。

## 0. 软件要求

* JDK 11/17：`brew install openjdk@11` 或 `brew install openjdk@17`
* Maven 3.X，且 Maven 必须指向 JDK 11 或 17。可通过 `mvn --version` 检查。
* Git 与 SVN

如果 `mvn --version` 显示 Maven 当前指向其他 JDK 版本，您必须把它切换到 JDK 11 或 17。步骤如下：

1. 列出本机所有已安装的 Java 版本：`/usr/libexec/java_home -V`。应能看到包含 JDK 11 与 17 的多个版本。
2. 运行 `whereis mvn` 获取 Maven 的安装位置。结果是一个指向真实位置的符号链接。
3. 在终端中打开该文件（必要时使用 `sudo`）。内容大致如下：

```
#!/bin/bash
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home)}" exec "/usr/local/Cellar/maven/3.6.3/libexec/bin/mvn" "$@"
```

4. 将 `JAVA_HOME:-$(/usr/libexec/java_home)}` 改为 `JAVA_HOME:-$(/usr/libexec/java_home -v 11)}` 或 `JAVA_HOME:-$(/usr/libexec/java_home -v 17)}`。修改后大致如下：

```
#!/bin/bash
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 11)}" exec "/usr/local/Cellar/maven/3.6.3/libexec/bin/mvn" "$@"
```

5. 再次运行 `mvn --version`，此时应已指向 JDK 11 或 17。

## 1. 获得 Sedona GitHub 仓库的写权限

1. 确认您的 GitHub 账号已启用 2FA：https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/
2. 在 Apache ID 资料中填写您的 GitHub ID：https://id.apache.org/
3. 通过 GitBox（Apache 账号关联工具）合并 Apache 与 GitHub 账号：https://gitbox.apache.org/setup/
	* GitBox 中应能看到 5 个绿色对勾。
	* 等待至少 30 分钟，您会收到一封邀请加入 Apache GitHub 组织的邮件，点击接受邀请即可。
4. 接受邀请后，确认您已在团队中：https://github.com/orgs/apache/teams/sedona-committers
5. 此外，如果您当选为 Sedona PMC 成员，还应确认自己出现在 LDAP 的 Sedona PMC 列表中：https://whimsy.apache.org/roster/pmc/sedona

## 2. 准备 GPG 私钥

1. 如果尚未安装 GNUPG，请先安装。Mac 上：`brew install gnupg gnupg2`。
2. 生成一个私钥，必须是 RSA4096（4096 位）。
   * 运行 `gpg --full-generate-key`；如果不可用，运行 `gpg --default-new-key-algo rsa4096 --gen-key`。
   * 提示选择密钥类型时：选 `RSA`，按 `enter`。
   * 提示选择密钥长度时：输入 `4096`。
   * 提示选择密钥有效期时：直接 `enter`，让密钥永不过期。
   * 确认所选项无误。
   * 输入用户身份信息：使用真实姓名与 Apache 邮箱。
   * 设置一个安全的口令（passphrase），请记住，后续会用到。
   * 运行 `gpg --list-secret-keys --keyid-format=long` 查看长格式的 GPG key 列表。
   * 从中复制您要使用的 key ID 长格式（如 `3AA5C34371567BD2`）。
   * 运行 `gpg --export --armor 3AA5C34371567BD2`，替换为您的 key ID。
   * 复制 `-----BEGIN PGP PUBLIC KEY BLOCK-----` 与 `-----END PGP PUBLIC KEY BLOCK-----` 之间的内容（即 armored key）。
   * `-----BEGIN PGP PUBLIC KEY BLOCK-----` 与实际 key 之间必须有一空行。
3. 将 armored key 发布到主流 key server：https://keyserver.pgp.com/

## 3. 使用 SVN 更新 KEYS

通过 SVN 将您的 armored 公钥追加到下面这两个 `KEYS` 文件中：

   * https://dist.apache.org/repos/dist/dev/sedona/KEYS
   * https://dist.apache.org/repos/dist/release/sedona/KEYS

1. 分别 check out 两个 KEYS 文件：

```bash
svn checkout https://dist.apache.org/repos/dist/dev/sedona/ sedona-dev --depth files
svn checkout https://dist.apache.org/repos/dist/release/sedona/ sedona-release --depth files
```

2. 使用您喜欢的文本编辑器打开 `sedona-dev/KEYS` 与 `sedona-release/KEYS`。
3. 把 armored key 粘贴到这两个文件末尾。注意：`-----BEGIN PGP PUBLIC KEY BLOCK-----` 与实际 key 之间必须有一空行。
4. 提交两个 KEYS。SVN 可能会要求输入您的 ASF ID 与密码——请输入，以便 SVN 在本地保存您的凭据。

```bash
svn commit -m "Update KEYS" sedona-dev/KEYS
svn commit -m "Update KEYS" sedona-release/KEYS
```

5. 然后删除两个 svn 工作目录：

```bash
rm -rf sedona-dev
rm -rf sedona-release
```

## 4. 添加 GPG_TTY 环境变量

在 `~/.bashrc` 中加入下列内容，然后重启终端：

```bash
GPG_TTY=$(tty)
export GPG_TTY
```

## 5. 获取 GitHub Personal Access Token（classic）

您需要创建一个 GitHub Personal Access Token（classic），可参考 [GitHub 官方文档](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token#creating-a-personal-access-token-classic)。

简要步骤：

1. 在 GitHub 界面进入 Settings。
2. 在左侧栏点击 Developer settings。
3. 在左侧栏的 Personal access tokens 下点击 Tokens (classic)。
4. 选择 Generate new token，然后点击 Generate new token (classic)。
5. 给 token 起一个有描述性的名字。
6. 设置 token 的过期时间——选择 Expiration 下拉菜单时，请设置为 `No expiration`。
7. 选择该 token 的权限范围。要从命令行访问仓库，请勾选 `repo` 与 `admin:org`。
8. 点击 `Generate token`。
9. 请把 token 保存好，下一步会用到。

## 6. 配置 Maven 凭据

在 `~/.m2/settings.xml` 中加入以下内容；如果该文件或 `.m2` 目录不存在，请先创建。

请将所有大写文本替换为您自己的 ID 与密码。

```
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>YOUR_GITHUB_TOKEN</password>
    </server>
    <server>
      <id>apache.snapshots.https</id>
      <username>YOUR_ASF_ID</username>
      <password>YOUR_ASF_PASSWORD</password>
    </server>
    <server>
      <id>apache.releases.https</id>
      <username>YOUR_ASF_ID</username>
      <password>YOUR_ASF_PASSWORD</password>
    </server>
  </servers>
  <profiles>
    <profile>
      <id>gpg</id>
      <properties>
        <gpg.passphrase>YOUR_GPG_PASSPHRASE</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>gpg</activeProfile>
  </activeProfiles>
</settings>
```
