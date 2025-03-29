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

# Become a release manager

You only need to perform these steps if this is your first time being a release manager.

## 0. Software requirement

* JDK 8: `brew install openjdk@8`
* Maven 3.X. Your Maven must point to JDK 8 (1.8). Check it by `mvn --version`
* Git and SVN

If your Maven (`mvn --version`) points to other JDK versions, you must change it to JDK 8. Steps are as follows:

1. Find all Java installed on your machine: `/usr/libexec/java_home -V`. You should see multiple JDK versions including JDK 8.
2. Run `whereis mvn` to get the installation location of your Maven. The result is a symlink to the actual location.
3. Open it in the terminal (with `sudo` if needed). It will be like this

```
#!/bin/bash
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home)}" exec "/usr/local/Cellar/maven/3.6.3/libexec/bin/mvn" "$@"
```

4. Change `JAVA_HOME:-$(/usr/libexec/java_home)}` to `JAVA_HOME:-$(/usr/libexec/java_home -v 1.8)}`. The resulting content will be like this:

```
#!/bin/bash
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 1.8)}" exec "/usr/local/Cellar/maven/3.6.3/libexec/bin/mvn" "$@"
```

5. Run `mvn --version` again. It should now point to JDK 8.

## 1. Obtain Write Access to Sedona GitHub repo

1. Verify you have a GitHub ID enabled with 2FA https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/
2. Enter your GitHub ID into your Apache ID profile https://id.apache.org/
3. Merge your Apache and GitHub accounts using GitBox (Apache Account Linking utility): https://gitbox.apache.org/setup/
	* You should see 5 green checks in GitBox
	* Wait at least 30  minutes for an email inviting you to Apache GitHub Organization and accept invitation
4. After accepting the GitHub Invitation, verify that you are a member of the team https://github.com/orgs/apache/teams/sedona-committers
5. Additionally, if you have been elected to the Sedona PMC, verify you are part of the LDAP Sedona PMC https://whimsy.apache.org/roster/pmc/sedona

## 2. Prepare Secret GPG key

1. Install GNUPG if it was not installed before. On Mac: `brew install gnupg gnupg2`
2. Generate a secret key. It must be RSA4096 (4096 bits long).
   * Run `gpg --full-generate-key`. If not work, run `gpg --default-new-key-algo rsa4096 --gen-key`
   * At the prompt, specify the kind of key you want: Select `RSA`, then press `enter`
   * At the prompt, specify the key size you want: Enter `4096`
   * At the prompt, enter the length of time the key should be valid: Press `enter` to make the key never expire.
   * Verify that your selections are correct.
   * Enter your user ID information: use your real name and Apache email address.
   * Type a secure passphrase. Make sure you remember this because we will use it later.
   * Use the `gpg --list-secret-keys --keyid-format=long` command to list the long form of the GPG keys.
   * From the list of GPG keys, copy the long form of the GPG key ID you'd like to use (e.g., `3AA5C34371567BD2`)
   * Run `gpg --export --armor 3AA5C34371567BD2`, substituting in the GPG key ID you'd like to use.
   * Copy your GPG key, beginning with `-----BEGIN PGP PUBLIC KEY BLOCK-----` and ending with `-----END PGP PUBLIC KEY BLOCK-----`.
   * There must be an empty line between `-----BEGIN PGP PUBLIC KEY BLOCK-----` and the actual key.
3. Publish your armored key in major key servers: https://keyserver.pgp.com/

## 3. Use SVN to update KEYS

Use SVN to append your armored PGP public key to the `KEYS` files

   * https://dist.apache.org/repos/dist/dev/sedona/KEYS
   * https://dist.apache.org/repos/dist/release/sedona/KEYS

1. Check out both KEYS files

```bash
svn checkout https://dist.apache.org/repos/dist/dev/sedona/ sedona-dev --depth files
svn checkout https://dist.apache.org/repos/dist/release/sedona/ sedona-release --depth files
```

2. Use your favorite text editor to open `sedona-dev/KEYS` and `sedona-release/KEYS`.
3. Paste your armored key to the end of both files. Note: There must be an empty line between `-----BEGIN PGP PUBLIC KEY BLOCK-----` and the actual key.
4. Commit both KEYS. SVN might ask you to enter your ASF ID and password. Make sure you do it so SVN can always store your ID and password locally.

```bash
svn commit -m "Update KEYS" sedona-dev/KEYS
svn commit -m "Update KEYS" sedona-release/KEYS
```

5. Then remove both svn folders

```bash
rm -rf sedona-dev
rm -rf sedona-release
```

## 4. Add GPG_TTY environment variable

In your `~/.bashrc` file, add the following content. Then restart your terminal.

```bash
GPG_TTY=$(tty)
export GPG_TTY
```

## 5. Get GitHub personal access token (classic)

You need to create a GitHub personal access token (classic). You can follow the instruction on [GitHub](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token#creating-a-personal-access-token-classic).

In short:

1. On your GitHub interface -> Settings
2. In the left sidebar, click Developer settings.
3. In the left sidebar, under  Personal access tokens, click Tokens (classic).
4. Select Generate new token, then click Generate new token (classic).
5. Give your token a descriptive name.
6. To give your token an expiration, select the Expiration drop-down menu. Make sure you set the `Expiration` to `No expiration`.
7. Select the scopes you'd like to grant this token. To use your token to access repositories from the command line, select `repo` and `admin:org`.
8. Click `Generate token`.
9. Please save your token somewhere because we will use it in the next step.

## 6. Set up credentials for Maven

In your `~/.m2/settings.xml` file, add the following content. Please create this file or `.m2` folder if it does not exist.

Please replace all capitalized text with your own ID and password.

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
