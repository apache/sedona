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

# Make a Sedona release

This page is for Sedona PMC to publish Sedona releases.

!!!warning
    All scripts on this page should be run in your local Sedona Git repo under master branch via a single script file.

## 0. Prepare an empty script file

1. In your local Sedona Git repo under master branch, run

```bash
echo '#!/bin/bash' > create-release.sh
chmod 777 create-release.sh
```

2. Use your favourite GUI text editor to open `create-release.sh`.
3. Then keep copying the scripts on this web page to replace all content in this script file.
4. Do NOT directly copy/paste the scripts to your terminal because a bug in `clipboard.js` will create link breaks in such case.
5. Each time when you copy content to this script file, run `./create-release.sh` to execute it.

## 1. Check ASF copyright in all file headers

1. Run the following script:

```bash
#!/bin/bash
wget -q https://archive.apache.org/dist/creadur/apache-rat-0.15/apache-rat-0.15-bin.tar.gz
tar -xvf  apache-rat-0.15-bin.tar.gz
git clone --shared --branch master https://github.com/apache/sedona.git sedona-src
java -jar apache-rat-0.15/apache-rat-0.15.jar -d sedona-src > report.txt
```

2. Read the generated report.txt file and make sure all source code files have ASF header.
3. Delete the generated report and cloned files

```bash
#!/bin/bash
rm -rf apache-rat-0.15
rm -rf sedona-src
rm report.txt
```

## 2. Update Sedona Python, R and Zeppelin versions

Make sure the Sedona version in the following files are {{ sedona_create_release.current_version }}.

1. https://github.com/apache/sedona/blob/master/python/sedona/version.py
2. https://github.com/apache/sedona/blob/master/R/DESCRIPTION
3. https://github.com/apache/sedona/blob/99239524f17389fc4ae9548ea88756f8ea538bb9/R/R/dependencies.R#L42
4. https://github.com/apache/sedona/blob/master/zeppelin/package.json

## 3. Update mkdocs.yml

* Please change the following variables in `mkdocs.yml` to the version you want to publish.
    * `sedona_create_release.current_version`
    * `sedona_create_release.current_rc`
    * `sedona_create_release.current_git_tag`
    * `sedona_create_release.current_snapshot`
* Then compile the website by `mkdocs serve`. This will generate the scripts listed on this page in your local browser.
* You can also publish this website if needed. See the instruction at bottom.

## 4. Stage and upload release candidates

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

## Note that we use maven-release-plugin 2.3.2 instead of more recent version (e.g., 3.0.1) to get rid of a bug of maven-release-plugin,
## which prevent us from cloning git repo with user specified -Dtag=<tag>.
## Please refer to https://issues.apache.org/jira/browse/MRELEASE-933 and https://issues.apache.org/jira/browse/SCM-729 for details.
##
## Please also note that system properties `-Dspark` and `-Dscala` has to be specified both for release:perform and the actual build parameters
## in `-Darguments`, because the build profiles activated for release:perform task will also affect the actual build task. It is safer to specify
## these system properties for both tasks.

# Define repository details
REPO_URL="https://github.com/apache/sedona.git"
RC_VERSION="{{ sedona_create_release.current_rc }}"
SEDONA_VERSION="{{ sedona_create_release.current_version }}"
TAG="sedona-${RC_VERSION}"
LOCAL_DIR="sedona-release"

# Remove existing directory if it exists and clone the repository
rm -rf $LOCAL_DIR && git clone --depth 1 --branch $TAG $REPO_URL $LOCAL_DIR && cd $LOCAL_DIR

# Define the Maven release plugin version
MAVEN_PLUGIN_VERSION="2.3.2"

# Define Spark and Scala versions
declare -a SPARK_VERSIONS=("3.4" "3.5" "4.0")
declare -a SCALA_VERSIONS=("2.12" "2.13")

# Function to get Java version for Spark version
get_java_version() {
  local spark_version=$1
  if [[ "$spark_version" == "4.0" ]]; then
    echo "17"
  else
    echo "11"
  fi
}

# Function to find Maven installation path
find_maven_path() {
  # Try different methods to find Maven
  local mvn_path=""

  # Method 1: Check if mvn is in PATH
  if command -v mvn >/dev/null 2>&1; then
    mvn_path=$(command -v mvn)
  fi

  # Method 2: Check common Homebrew locations
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /opt/homebrew/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # Method 3: Check /usr/local (older Homebrew installations)
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /usr/local/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # Method 4: Check system locations
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

# Function to create Maven wrapper with specific Java version
create_mvn_wrapper() {
  local java_version=$1
  local mvn_wrapper="/tmp/mvn-java${java_version}"
  local mvn_path=$(find_maven_path)

  echo "Using Maven at: $mvn_path" >&2

  # Create a wrapper script that sets JAVA_HOME and executes Maven
  cat > "$mvn_wrapper" << EOF
#!/bin/bash
JAVA_HOME="\${JAVA_HOME:-\$(/usr/libexec/java_home -v ${java_version})}" exec "${mvn_path}" "\$@"
EOF

  chmod +x "$mvn_wrapper"
  echo "$mvn_wrapper"
}

# Function to verify Java version using Maven wrapper
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

# Iterate through Spark and Scala versions
for SPARK in "${SPARK_VERSIONS[@]}"; do
  for SCALA in "${SCALA_VERSIONS[@]}"; do
    # Skip Spark 4.0 + Scala 2.12 combination as it's not supported
    if [[ "$SPARK" == "4.0" && "$SCALA" == "2.12" ]]; then
      echo "Skipping Spark $SPARK with Scala $SCALA (not supported)"
      continue
    fi

    JAVA_VERSION=$(get_java_version $SPARK)
    echo "Running release:perform for Spark $SPARK and Scala $SCALA with Java $JAVA_VERSION..."

    # Create Maven wrapper with appropriate Java version
    MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
    echo "Created Maven wrapper: $MVN_WRAPPER"

    # Verify Java version
    verify_java_version $MVN_WRAPPER $JAVA_VERSION

    # Execute Maven with the wrapper
    $MVN_WRAPPER org.apache.maven.plugins:maven-release-plugin:$MAVEN_PLUGIN_VERSION:perform \
      -DconnectionUrl=scm:git:file://$(pwd) \
      -Dtag=$TAG \
      -Dresume=false \
      -Darguments="-DskipTests -Dspark=$SPARK -Dscala=$SCALA" \
      -Dspark=$SPARK \
      -Dscala=$SCALA

    # Clean up the wrapper
    rm -f $MVN_WRAPPER
  done
done

echo "*****Step 3: Upload Release Candidate on ASF SVN: https://dist.apache.org/repos/dist/dev/sedona"

echo "Creating ${RC_VERSION} folder on SVN..."

svn mkdir -m "Adding folder" https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}

echo "Creating release files locally..."

# Go back to parent directory for file operations
cd ../..

echo "Downloading source code..."

wget https://github.com/apache/sedona/archive/refs/tags/sedona-${RC_VERSION}.tar.gz
tar -xvf sedona-${RC_VERSION}.tar.gz
mkdir apache-sedona-${SEDONA_VERSION}-src
cp -r sedona-sedona-${RC_VERSION}/* apache-sedona-${SEDONA_VERSION}-src/
tar czf apache-sedona-${SEDONA_VERSION}-src.tar.gz apache-sedona-${SEDONA_VERSION}-src
rm sedona-${RC_VERSION}.tar.gz
rm -rf sedona-sedona-${RC_VERSION}

# Create checksums and signatures for source files
shasum -a 512 apache-sedona-${SEDONA_VERSION}-src.tar.gz > apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512
gpg -ab apache-sedona-${SEDONA_VERSION}-src.tar.gz

echo "Uploading source files..."

# Upload source files first
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-src.tar.gz https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-src.tar.gz
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-src.tar.gz.asc https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-src.tar.gz.asc
svn import -m "Adding file" apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/sedona/${RC_VERSION}/apache-sedona-${SEDONA_VERSION}-src.tar.gz.sha512

echo "Compiling the source code..."

mkdir apache-sedona-${SEDONA_VERSION}-bin

# Function to get Java version for Spark version
get_java_version() {
  local spark_version=$1
  if [[ "$spark_version" == "4.0" ]]; then
    echo "17"
  else
    echo "11"
  fi
}

# Function to find Maven installation path
find_maven_path() {
  # Try different methods to find Maven
  local mvn_path=""

  # Method 1: Check if mvn is in PATH
  if command -v mvn >/dev/null 2>&1; then
    mvn_path=$(command -v mvn)
  fi

  # Method 2: Check common Homebrew locations
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /opt/homebrew/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # Method 3: Check /usr/local (older Homebrew installations)
  if [[ -z "$mvn_path" ]]; then
    for version_dir in /usr/local/Cellar/maven/*/libexec/bin/mvn; do
      if [[ -x "$version_dir" ]]; then
        mvn_path="$version_dir"
        break
      fi
    done
  fi

  # Method 4: Check system locations
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

# Function to create Maven wrapper with specific Java version
create_mvn_wrapper() {
  local java_version=$1
  local mvn_wrapper="/tmp/mvn-java${java_version}"
  local mvn_path=$(find_maven_path)

  echo "Using Maven at: $mvn_path" >&2

  # Create a wrapper script that sets JAVA_HOME and executes Maven
  cat > "$mvn_wrapper" << EOF
#!/bin/bash
JAVA_HOME="\${JAVA_HOME:-\$(/usr/libexec/java_home -v ${java_version})}" exec "${mvn_path}" "\$@"
EOF

  chmod +x "$mvn_wrapper"
  echo "$mvn_wrapper"
}

# Function to verify Java version using Maven wrapper
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

# Compile for Spark 3.4 with Java 11
JAVA_VERSION=$(get_java_version "3.4")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 3.4 with Scala 2.12 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.4 -Dscala=2.12 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

echo "Compiling for Spark 3.4 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.4 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# Compile for Spark 3.5 with Java 11
JAVA_VERSION=$(get_java_version "3.5")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 3.5 with Scala 2.12 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.5 -Dscala=2.12 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

echo "Compiling for Spark 3.5 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=3.5 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# Compile for Spark 4.0 with Java 17
JAVA_VERSION=$(get_java_version "4.0")
MVN_WRAPPER=$(create_mvn_wrapper $JAVA_VERSION)
verify_java_version $MVN_WRAPPER $JAVA_VERSION

echo "Compiling for Spark 4.0 with Scala 2.13 using Java $JAVA_VERSION..."
cd apache-sedona-${SEDONA_VERSION}-src && $MVN_WRAPPER clean && $MVN_WRAPPER install -DskipTests -Dspark=4.0 -Dscala=2.13 && cd ..
cp apache-sedona-${SEDONA_VERSION}-src/spark-shaded/target/sedona-*${SEDONA_VERSION}.jar apache-sedona-${SEDONA_VERSION}-bin/

# Clean up Maven wrappers
rm -f /tmp/mvn-java11 /tmp/mvn-java17

tar czf apache-sedona-${SEDONA_VERSION}-bin.tar.gz apache-sedona-${SEDONA_VERSION}-bin

# Create checksums and signatures for binary files
shasum -a 512 apache-sedona-${SEDONA_VERSION}-bin.tar.gz > apache-sedona-${SEDONA_VERSION}-bin.tar.gz.sha512
gpg -ab apache-sedona-${SEDONA_VERSION}-bin.tar.gz

echo "Uploading binary files..."

# Upload binary files
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

## 5. Vote in dev sedona.apache.org

### Vote email

Please add changes at the end if needed:

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

### Pass email

Please count the votes and add the Permalink of the vote thread at the end.

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

### Announce email

1. This email should be sent to dev@sedona.apache.org
2. Please add the permalink of the vote thread
3. Please add the permalink of the vote result thread

```
Subject: [ANNOUNCE] Apache Sedona {{ sedona_create_release.current_version }} released

Dear all,

We are happy to report that we have released Apache Sedona {{ sedona_create_release.current_version }}. Thank you again for your help.

Apache Sedona is a cluster computing system for processing large-scale spatial data.


Vote thread (Permalink from https://lists.apache.org/list.html):


Vote result thread (Permalink from https://lists.apache.org/list.html):


Website:
http://sedona.apache.org/

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

## 7. Failed vote

If a vote failed, do the following:

1. In the vote email, say that we will create another release candidate.
2. Restart from Step 3 `Update mkdocs.yml`. Please increment the release candidate ID (e.g., `{{ sedona_create_release.current_version}}-rc2`) and update `sedona_create_release.current_rc` and `sedona_create_release.current_git_tag` in `mkdocs.yml` to generate the script listed on this webpage.

## 8. Release source code and Maven package

### Upload releases

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

### Manually close and release the package

1. Click `Close` on the Sedona staging repo on https://repository.apache.org under `staging repository`
2. Once the staging repo is closed, click `Release` on this repo.

## 9. Release Sedona Python

Sedona GitHub CI will automatically publish wheel files to PyPi once a GitHub release is created.

## 10. Release Sedona Zeppelin

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

## 11. Release Sedona R to CRAN.

```bash
#!/bin/bash
R CMD build .
R CMD check --as-cran apache.sedona_*.tar.gz
```

Then submit to CRAN using this [web form](https://xmpalantir.wu.ac.at/cransubmit/).

## 12. Publish the doc website

1. Check out the {{ sedona_create_release.current_version }} Git tag on your local repo to a branch namely `branch-{{ sedona_create_release.current_version }}`
2. Add the download link to [Download page](../download.md).
3. Add the news to `docs/index.md`.
4. Push the changes to this branch on GitHub.
5. GitHub CI will pick up the changes and deploy to `website` branch.
6. Normally [this repo](https://github.com/jiayuasu/sedona-sync-action) will automatically publish the website on a daily basis.
