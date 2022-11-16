# Publish a SNAPSHOT version

This step is to publish Maven SNAPSHOTs to https://repository.apache.org

This is a good practice for a release manager to try out his/her credential setup.

The detailed requirement is on [ASF Infra website](https://infra.apache.org/publishing-maven-artifacts.html)

!!!warning
All scripts on this page should be run in your local Sedona Git repo under master branch via a single script file.

## 0. Prepare an empty script file

1. In your local Sedona Git repo under master branch, run
```bash
echo "#!/bin/bash" > create-release.sh
chmod 777 create-release.sh
```
2. Use your favourite GUI text editor to open `create-release.sh`.
3. Then keep copying the scripts on this web page to replace all content in this text file.
4. Do NOT directly copy/paste the scripts to your terminal because a bug in `clipboard.js` will create link breaks in such case.

## 1. Upload snapshot versions

In your Sedona GitHub repo, run this script:

```bash
#!/bin/bash

git checkout master
git pull

# Spark 3.0 and Scala 2.12
# Prepare the SNAPSHOTs
mvn -q clean -Darguments="-DskipTests" release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false
# Deploy the SNAPSHOTs
mvn -q deploy -DskipTests

# Prepare for Spark 3.0 and Scala 2.13
# Prepare the SNAPSHOTs
mvn -q clean -Darguments="-DskipTests -Dscala=2.13" release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false
# Deploy the SNAPSHOTs
mvn -q deploy -DskipTests -Dscala=2.13
```