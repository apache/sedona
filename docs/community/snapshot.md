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

# Publish a SNAPSHOT version

This step is to publish Maven SNAPSHOTs to https://repository.apache.org

This is a good practice for a release manager to try out his/her credential setup.

The detailed requirement is on [ASF Infra website](https://infra.apache.org/publishing-maven-artifacts.html)

!!!warning
    All scripts on this page should be run in your local Sedona Git repo under master branch via a single script file.

## 0. Prepare an empty script file

1. In your local Sedona Git repo under master branch, run

```bash
echo '#!/bin/bash' > create-release.sh
chmod 777 create-release.sh
```

2. Use your favourite GUI text editor to open `create-release.sh`.
3. Then keep copying the scripts on this web page to replace all content in this text file.
4. Do NOT directly copy/paste the scripts to your terminal because a bug in `clipboard.js` will create link breaks in such case.
5. Each time when you copy content to this script file, run `./create-release.sh` to execute it.

## 1. Upload snapshot versions

In your Sedona GitHub repo, run this script:

```bash
#!/bin/bash

git checkout master
git pull

rm -f release.*
rm -f pom.xml.*

# Validate the POMs and your credential setup
mvn -q -B clean release:prepare -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.current_snapshot }} -Dresume=false -DdryRun=true -Penable-all-submodules -Darguments="-DskipTests"
mvn -q -B release:clean -Penable-all-submodules

# Spark 3.3 and Scala 2.12
mvn -q deploy -DskipTests -Dspark=3.3 -Dscala=2.12

# Spark 3.3 and Scala 2.13
mvn -q deploy -DskipTests -Dspark=3.3 -Dscala=2.13

# Spark 3.4 and Scala 2.12
mvn -q deploy -DskipTests -Dspark=3.4 -Dscala=2.12

# Spark 3.4 and Scala 2.13
mvn -q deploy -DskipTests -Dspark=3.4 -Dscala=2.13
```
