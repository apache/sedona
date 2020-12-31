# Compile and Publish Sedona

[![Scala and Java build](https://github.com/apache/incubator-sedona/workflows/Scala%20and%20Java%20build/badge.svg)](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Scala+and+Java+build%22) [![Python build](https://github.com/apache/incubator-sedona/workflows/Python%20build/badge.svg)](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Python+build%22)


## Compile Scala and Java source code
Sedona Scala/Java code is a project with four modules, core, sql, viz and python adapter. Each module is a Scala/Java mixed project which is managed by Apache Maven 3.

* Make sure your machine has Java 1.8 and Apache Maven 3.

To compile all modules, please make sure you are in the root folder of three modules. Then enter the following command in the terminal:

```
mvn clean install -DskipTests
```
This command will first delete the old binary files and compile all modules. This compilation will skip the unit tests. To compile a single module, please make sure you are in the folder of that module. Then enter the same command.

!!!note
	By default, this command will compile Sedona with Spark 3.0 and Scala 2.12

To run unit tests, just simply remove `-DskipTests` option. The command is like this:
```
mvn clean install
```

!!!warning
	The unit tests of all three modules may take up to 30 minutes. 

### Compile with different targets

* Spark 3.0 + Scala 2.12
```
mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.0
```
* Spark 2.4 + Scala 2.11
```
mvn clean install -DskipTests -Dscala=2.11 -Dspark=2.4
```
* Spark 2.4 + Scala 2.12
```
mvn clean install -DskipTests -Dscala=2.12 -Dspark=2.4
```

### Download staged jars

Sedona uses GitHub action to automatically generate jars per commit. You can go [here](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Scala+and+Java+build%22) and download the jars by clicking the commit's ==Artifacts== tag.

## Run Python test

1. Set up the environment variable SPARK_HOME and PYTHONPATH

For example,
```
export SPARK_HOME=$PWD/spark-3.0.1-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python
```
2. Compile the Sedona Scala and Java code and then copy the ==sedona-python-adapter-xxx.jar== to ==SPARK_HOME/jars/== folder
```
cp python-adapter/target/sedona-python-adapter-xxx.jar SPARK_HOME/jars/
```
3. Install the following libraries
```
sudo apt-get -y install python3-pip python-dev libgeos-dev
sudo pip3 install -U setuptools
sudo pip3 install -U wheel
sudo pip3 install -U virtualenvwrapper
sudo pip3 install -U pipenv
```
4. Set up pipenv to the desired Python version: 3.7, 3.8, or 3.9
```
cd python
pipenv --python 3.7
```
5. Install the PySpark version and other dependency
```
cd python
pipenv install pyspark==3.0.1
pipenv install --dev
```
6. Run the Python tests
```
cd python
pipenv run pytest tests
```
## Compile the documentation

### Website

#### Compile

The source code of the documentation website is written in Markdown and then compiled by MkDocs. The website is built upon [Material for MkDocs template](https://squidfunk.github.io/mkdocs-material/).

In the Sedona repository, MkDocs configuration file ==mkdocs.yml== is in the root folder and all documentation source code is in docs folder.

To compile the source code and test the website on your local machine, please read [MkDocs Tutorial](http://www.mkdocs.org/#installation) and [Materials for MkDocs Tutorial](https://squidfunk.github.io/mkdocs-material/getting-started/).

In short, you need to run:

```
pip install mkdocs
pip install mkdocs-material
```

After installing MkDocs and MkDocs-Material, run the command in Sedona root folder:

```
mkdocs serve
```

#### Deploy to ASF domain

1. Run `mkdocs build` in Sedona root directory. Copy all content in the `site` folder.
2. Check out GitHub incubator-sedona-website [asf-site branch](https://github.com/apache/incubator-sedona-website/tree/asf-site)
3. Use the copied content to replace all content in `asf-site` branch and upload to GitHub. Then [sedona.apache.org](https:/sedona.apache.org) wil be automatically updated.
4. You can also push the content to `asf-staging` branch. The staging website will be then updated: [sedona.staged.apache.org](https:/sedona.staged.apache.org)

### Javadoc and Scaladoc

#### Compile

You should first compile the entire docs using `mkdocs build` to get the `site` folder.

* Javadoc: Use Intelij IDEA to generate Javadoc for `core` and `viz` module
* Scaladoc: Run `scaladoc -d site/api/javadoc/sql/ sql/src/main/scala/org/apache/sedona/sql/utils/*.scala`

#### Copy

Copy the generated Javadoc (Scaladoc should already be there) to the corresponding folders in `site/api/javadoc`

#### Deploy to ASF domain

1. Copy the generated Javadoc and Scaladoc to the correct location in `docs/api/javadoc`

2. Then deploy Javadoc and Scaladoc with the project website

## Publish SNAPSHOTs

We follow the [ASF Incubator Distribution Guidelines](https://incubator.apache.org/guides/distribution.html)

### Publish Maven project to ASF repository

The detailed requirement is on [ASF Infra website](https://infra.apache.org/publishing-maven-artifacts.html)

#### Prepare for Spark 3.0 and Scala 2.12

1. Convert source code to Spark 3 format
```
python3 spark-version-converter.py spark3
```
2. Prepare the SNAPSHOTs
```
mvn clean -Darguments="-DskipTests" release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false
```
3. Deploy the SNAPSHOTs
```
mvn deploy -DskipTests
```

#### Prepare for Spark 2.4 and Scala 2.11

1. Convert source code to Spark 2 format
```
python3 spark-version-converter.py spark2
```
2. Prepare the SNAPSHOTs
```
mvn clean release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false -DcheckModificationExcludeList=sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala -Darguments="-DskipTests -Dscala=2.11 -Dspark=2.4"
```
3. Deploy the SNAPSHOTs
```
mvn deploy -DskipTests -Dscala=2.11 -Dspark=2.4
```

#### Prepare for Spark 2.4 and Scala 2.12

1. Convert source code to Spark 2 format
```
python3 spark-version-converter.py spark2
```
2. Prepare the SNAPSHOTs
```
mvn clean release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false -DcheckModificationExcludeList=sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala -Darguments="-DskipTests -Dscala=2.12 -Dspark=2.4"
```
3. Deploy the SNAPSHOTs
```
mvn deploy -DskipTests -Dscala=2.12 -Dspark=2.4
```

### Publish Python project to PyPi

## Publish releases

### Stage the releases

#### For Spark 3.0 and Scala 2.12

1. Convert source code to Spark 3 format
```bash
python3 spark-version-converter.py spark3
```
2. Prepare a release. Manually enter the following variables in the terminal: release id: ==1.0.0-incubator==, scm tag id: ==sedona-3.0_2.12-1.0.0-incubator== (this is just an example. Please use the correct version number). You also need to provide GitHub username and password
```bash
mvn clean release:prepare -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests" 
```
3. Stage a release
```bash
mvn clean release:perform -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests" 
```
4. Now the releases are staged. A tag and two commits will be created on Sedona GitHub repo.

Now let's repeat the process to other Sedona modules.

#### For Spark 2.4 and Scala 2.11

1. Convert source code to Spark 2 format
```bash
python3 spark-version-converter.py spark2
```
2. Manuallly commit the changes of the three scala files to GitHub
3. Prepare a release. Note that: release id: ==1.0.0-incubator==, scm tag id: ==sedona-2.4_2.11-1.0.0-incubator== (this is just an example. Please use the correct version number)
```bash
mvn clean release:prepare -DautoVersionSubmodules=true -Dresume=false -DcheckModificationExcludeList=sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala -Darguments="-DskipTests -Dscala=2.11 -Dspark=2.4"
```
4. Stage a release
```bash
mvn clean release:perform -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.11 -Dspark=2.4"
```

#### For Spark 2.4 and Scala 2.12

Step 1 and 2 are only needed if you didn't run the previous step before

1. Convert source code to Spark 2 format
```bash
python3 spark-version-converter.py spark2
```
2. ==Manuallly commit the changes of the three scala files to GitHub==
3. Prepare a release: release id: ==1.0.0-incubator==, scm tag id: ==sedona-2.4_2.12-1.0.0-incubator==  (this is just an example. Please use the correct version number)
```bash
mvn clean release:prepare -DautoVersionSubmodules=true -Dresume=false -DcheckModificationExcludeList=sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala -Darguments="-DskipTests -Dscala=2.12 -Dspark=2.4"
```
4. Stage a release
```bash
mvn clean release:perform -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.12 -Dspark=2.4"
```

!!!warning
	After staged the three releases, you need to manually revert the commited three scala files. You will see 6 [maven-release-plugin] commits and 3 more tags in Sedona GitHub repo.

### Close the staging repo
1. Check the status of the staging repo: [Locate and Examine Your Staging Repository
](https://central.sonatype.org/pages/releasing-the-deployment.html#locate-and-examine-your-staging-repository). You should see 12 Sedona modules in total.
2. Call for a vote in Sedona community and Apache incubator. Then close the staging repo.