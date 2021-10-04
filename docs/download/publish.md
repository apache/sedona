# Publish Sedona

## Check ASF copyright in all file headers

1. Download [Apache Rat binary (.jar file)](https://creadur.apache.org/rat/download_rat.cgi).
2. Run the following terminal script:
```bash
#!/bin/bash
git clone --shared --branch master https://github.com/apache/incubator-sedona.git sedona-src
java -jar apache-rat-0.13.jar -d sedona-src > report.txt
```
3. Read the generated report.txt file and make sure all source code files have ASF header.
4. Delete the generated report and cloned files
```
rm -rf sedona-src
rm report.txt
```

## Publish the doc website

1. Run `mkdocs build` in Sedona root directory. Copy all content in the `site` folder.
2. Check out GitHub incubator-sedona-website [asf-site branch](https://github.com/apache/incubator-sedona-website/tree/asf-site)
3. Use the copied content to replace all content in `asf-site` branch and upload to GitHub. Then `sedona.apache.org` will be automatically updated.
4. You can also push the content to `asf-staging` branch. The staging website will be then updated: `sedona.staged.apache.org`

### Javadoc and Scaladoc

#### Compile

You should first compile the entire docs using `mkdocs build` to get the `site` folder.

* Javadoc: Use Intelij IDEA to generate Javadoc for `core` and `viz` module
* Scaladoc: Run `scaladoc -d site/api/javadoc/sql/ sql/src/main/scala/org/apache/sedona/sql/utils/*.scala`

#### Copy

1. Copy the generated Javadoc (Scaladoc should already be there) to the corresponding folders in `site/api/javadoc`
2. Deploy Javadoc and Scaladoc with the project website


### Compile R html docs

1. Make sure you install R, tree and curl on your Ubuntu machine
```
sudo apt install littler tree libcurl4-openssl-dev
```
2. In the `R` directory, run `Rscript generate-docs.R`. This will create `rdocs` folder in Sedona `/docs/api/rdocs`

3. In `/docs/api/rdocs`, run `tree -H '.' -L 1 --noreport --charset utf-8 -o index.html` to generate `index.html`


!!!note
	Please read the following guidelines first: 1. ASF Incubator Distribution Guidelines: https://incubator.apache.org/guides/distribution.html 2. ASF Release Guidelines: https://infra.apache.org/release-publishing.html 3. ASF Incubator Release Votes Guidelines: https://issues.apache.org/jira/browse/LEGAL-469

## Publish SNAPSHOTs

### Publish Maven SNAPSHOTs

This step is to publish the SNAPSHOTs to https://repository.apache.org

The detailed requirement is on [ASF Infra website](https://infra.apache.org/publishing-maven-artifacts.html)

#### Prepare for Spark 3.0 and Scala 2.12

1. Prepare the SNAPSHOTs
```
mvn clean -Darguments="-DskipTests" release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false
```
2. Deploy the SNAPSHOTs
```
mvn deploy -DskipTests
```

#### Prepare for Spark 2.4 and Scala 2.11

1. Prepare the SNAPSHOTs
```
mvn clean release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false -DcheckModificationExcludeList=sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala -Darguments="-DskipTests -Dscala=2.11 -Dspark=2.4"
```
2. Deploy the SNAPSHOTs
```
mvn deploy -DskipTests -Dscala=2.11 -Dspark=2.4
```

#### Prepare for Spark 2.4 and Scala 2.12

1. Prepare the SNAPSHOTs
```
mvn clean release:prepare -DdryRun=true -DautoVersionSubmodules=true -Dresume=false -DcheckModificationExcludeList=sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala,sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala -Darguments="-DskipTests -Dscala=2.12 -Dspark=2.4"
```
2. Deploy the SNAPSHOTs
```
mvn deploy -DskipTests -Dscala=2.12 -Dspark=2.4
```

## Publish releases

### Stage the Release Candidate

This step is to stage the release to https://repository.apache.org

#### For Spark 3.0 and Scala 2.12

1. Prepare a release. Manually enter the following variables in the terminal: release id: `{{ sedona.current_version }}`, scm tag id: `{{ sedona.current_git_tag }}`. You also need to provide GitHub username and password three times.
```bash
mvn clean release:prepare -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests"
```
2. Stage a release
```bash
mvn clean release:perform -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests" 
```
3. Now the releases are staged. A tag and two commits will be created on Sedona GitHub repo.

Now let's repeat the process to other Sedona modules. Make sure you use the correct SCM Git tag id `{{ sedona.current_git_tag }}` (see below).

#### For Spark 2.4 and Scala 2.11

```
mvn org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/incubator-sedona.git -Dtag={{ sedona.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.11 -Dspark=2.4"
```

#### For Spark 2.4 and Scala 2.12

```
mvn org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/incubator-sedona.git -Dtag={{ sedona.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.12 -Dspark=2.4"
```

#### Use Maven Release Plugin directly from an existing tag

In some cases (i.e., the staging repo on repository.apache.org is closed by mistake), if you need to do `mvn release:perform` from an existing tag, you should use the following command. Note that: you have to use `org.apache.maven.plugins:maven-release-plugin:2.3.2:perform` due to a bug in maven release plugin from v2.4 (https://issues.apache.org/jira/browse/SCM-729). Make sure you use the correct scm tag (i.e.,  `{{ sedona.current_git_tag }}`).

##### For Spark 3.0 and Scala 2.12

```
mvn org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/incubator-sedona.git -Dtag={{ sedona.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests"
```

##### For Spark 2.4 and Scala 2.11

```
mvn org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/incubator-sedona.git -Dtag={{ sedona.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.11 -Dspark=2.4"
```

##### For Spark 2.4 and Scala 2.12

```
mvn org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/incubator-sedona.git -Dtag={{ sedona.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.12 -Dspark=2.4"
```

### Upload Release Candidate

All release candidates must be first placed in ASF Dist Dev SVN before vote: https://dist.apache.org/repos/dist/dev/incubator/sedona

1. Make sure your armored PGP public key (must be encrypted by RSA-4096) is included in the `KEYS` file: https://dist.apache.org/repos/dist/dev/incubator/sedona/KEYS, and publish in major key servers: https://pgp.mit.edu/, https://keyserver.ubuntu.com/, http://keys.gnupg.net/
2. Create a folder on SVN, such as `{{ sedona.current_git_tag }}`
```bash
#!/bin/bash
svn mkdir -m "Adding folder" https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}
```
3. In a folder other than the Sedona git repo, run the following script to create six files and two folders.
```bash
#!/bin/bash
git clone --shared --branch {{ sedona.current_git_tag}} https://github.com/apache/incubator-sedona.git apache-sedona-{{ sedona.current_version }}-src
rm -rf apache-sedona-{{ sedona.current_version }}-src/.git
tar czf apache-sedona-{{ sedona.current_version }}-src.tar.gz apache-sedona-{{ sedona.current_version }}-src
mkdir apache-sedona-{{ sedona.current_version }}-bin
cd apache-sedona-{{ sedona.current_version }}-src && mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.0 && cd ..
cp apache-sedona-{{ sedona.current_version }}-src/core/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/sql/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/viz/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/python-adapter/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cd apache-sedona-{{ sedona.current_version }}-src && mvn clean install -DskipTests -Dscala=2.11 -Dspark=2.4 && cd ..
cp apache-sedona-{{ sedona.current_version }}-src/core/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/sql/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/viz/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/python-adapter/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cd apache-sedona-{{ sedona.current_version }}-src && mvn clean install -DskipTests -Dscala=2.12 -Dspark=2.4 && cd ..
cp apache-sedona-{{ sedona.current_version }}-src/core/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/sql/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/viz/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
cp apache-sedona-{{ sedona.current_version }}-src/python-adapter/target/sedona-*{{ sedona.current_version}}.jar apache-sedona-{{ sedona.current_version }}-bin/
tar czf apache-sedona-{{ sedona.current_version }}-bin.tar.gz apache-sedona-{{ sedona.current_version }}-bin
shasum -a 512 apache-sedona-{{ sedona.current_version }}-src.tar.gz > apache-sedona-{{ sedona.current_version }}-src.tar.gz.sha512
shasum -a 512 apache-sedona-{{ sedona.current_version }}-bin.tar.gz > apache-sedona-{{ sedona.current_version }}-bin.tar.gz.sha512
gpg -ab apache-sedona-{{ sedona.current_version }}-src.tar.gz
gpg -ab apache-sedona-{{ sedona.current_version }}-bin.tar.gz
```
4. Upload six files to SVN and delete all created files using the following script
```bash
#!/bin/bash
svn import -m "Adding file" apache-sedona-{{ sedona.current_version }}-src.tar.gz https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-src.tar.gz
svn import -m "Adding file" apache-sedona-{{ sedona.current_version }}-src.tar.gz.asc https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-src.tar.gz.asc
svn import -m "Adding file" apache-sedona-{{ sedona.current_version }}-src.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-src.tar.gz.sha512
svn import -m "Adding file" apache-sedona-{{ sedona.current_version }}-bin.tar.gz https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-bin.tar.gz
svn import -m "Adding file" apache-sedona-{{ sedona.current_version }}-bin.tar.gz.asc https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-bin.tar.gz.asc
svn import -m "Adding file" apache-sedona-{{ sedona.current_version }}-bin.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-bin.tar.gz.sha512
rm apache-sedona-{{ sedona.current_version }}-src.tar.gz
rm apache-sedona-{{ sedona.current_version }}-src.tar.gz.asc
rm apache-sedona-{{ sedona.current_version }}-src.tar.gz.sha512
rm apache-sedona-{{ sedona.current_version }}-bin.tar.gz
rm apache-sedona-{{ sedona.current_version }}-bin.tar.gz.asc
rm apache-sedona-{{ sedona.current_version }}-bin.tar.gz.sha512
rm -rf apache-sedona-{{ sedona.current_version }}-src
rm -rf apache-sedona-{{ sedona.current_version }}-bin
```

### Call for a vote
1. Check the status of the staging repo: [Locate and Examine Your Staging Repository
](https://central.sonatype.org/pages/releasing-the-deployment.html#locate-and-examine-your-staging-repository). You should see 12 Sedona modules in total.
2. Call for a vote in Sedona community and Apache incubator. Then close the staging repo.

Here is a generated vote email. Please add changes at the end if needed:

```
Subject: [VOTE] Release Apache Sedona {{ sedona.current_rc }}

Hi all,

This is a call for vote on Apache Sedona {{ sedona.current_rc }}. Please refer to the changes listed at the bottom of this email.

Release notes:
https://github.com/apache/incubator-sedona/blob/{{ sedona.current_git_tag }}/docs/download/release-notes.md

Build instructions:
https://github.com/apache/incubator-sedona/blob/{{ sedona.current_git_tag }}/docs/download/compile.md

GitHub tag:
https://github.com/apache/incubator-sedona/releases/tag/{{ sedona.current_git_tag }}

GPG public key to verify the Release:
https://dist.apache.org/repos/dist/dev/incubator/sedona/KEYS

Source code and binaries:
https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/

The vote will be open for at least 72 hours or until a majority of at least 3 "approve" PMC votes are cast

Please vote accordingly:

[ ] +1 approve

[ ] +0 no opinion

[ ] -1 disapprove with the reason

Checklist for reference (because of DISCLAIMER-WIP, other checklist items are not blockers):

[ ] Download links are valid.

[ ] Checksums and PGP signatures are valid.

[ ] DISCLAIMER is included.

[ ] Source code artifacts have correct names matching the current release.

For a detailed checklist  please refer to:
https://cwiki.apache.org/confluence/display/INCUBATOR/Incubator+Release+Checklist

------------

Changes according to the comments on the previous release
Original comment (Permalink from https://lists.apache.org/list.html): 


```

Here is a generated "pass" email:

```
Subject: [RESULT][VOTE] Release Apache Sedona {{ sedona.current_rc }}

Dear all,

The vote closes now as 72hr have passed. The vote PASSES with
 
+? (PPMC): NAME1, NAME2, NAME3
+? (non-binding): NAME4
No -1 votes

The vote thread (Permalink from https://lists.apache.org/list.html):

I will now bring the vote to general@incubator.apache.org to get
approval by the IPMC. If this vote passes too, the release is accepted and will be published.

```

Here is a generated "announce" email:

```
Subject: [ANNOUNCE] Apache Sedona {{ sedona.current_version }} released

Dear all,

We are happy to report that we have released Apache Sedona (incubating) {{ sedona.current_version }}. Thank you again for your help.

Apache Sedona (incubating) is a cluster computing system for processing large-scale spatial data. 

Vote thread (Permalink from https://lists.apache.org/list.html):


Vote result thread (Permalink from https://lists.apache.org/list.html):


Website:
http://sedona.apache.org/

Release notes:
https://github.com/apache/incubator-sedona/blob/sedona-{{ sedona.current_version }}/docs/download/release-notes.md

Download links:
https://github.com/apache/incubator-sedona/releases/tag/sedona-{{ sedona.current_version }}

Additional resources:
Get started: http://sedona.apache.org/download/overview/
Tutorials: http://sedona.apache.org/tutorial/rdd/
Mailing list: dev@sedona.apache.org
Twitter: https://twitter.com/ApacheSedona
Gitter: https://gitter.im/apache/sedona

Regards,
Apache Sedona (incubating) Team
```

### Failed vote

If a vote failed, please first drop the staging repo on `repository.apache.org`. Then redo all the steps above. Make sure you use a new scm tag for the new release candidate when use maven-release-plugin (i.e., `{{ sedona.current_version}}-rc2`). You can change the `sedona.current_rc` and `sedona.current_git_tag` in `mkdocs.yml` to generate the script listed on this webpage.
 
### Release the package

1. Move all files in https://dist.apache.org/repos/dist/dev/incubator/sedona to https://dist.apache.org/repos/dist/release/incubator/sedona, using svn
2. Create a GitHub release. Please follow the template: https://github.com/apache/incubator-sedona/releases/tag/sedona-1.0.0-incubating
3. Publish Python project to PyPi using twine. You must have the maintainer priviledge of https://pypi.org/project/apache-sedona/. Then please setup your token and run `python3 setup.py sdist bdist_wheel` and `twine upload dist/*` in the `incubator-sedona/python` directory.
4. Publish Sedona-Zeppelin (a node.js package) on NPM. Run `npm publish` in the `zeppelin` directory.
5. Close the staging repo on https://repository.apache.org. If the staging repo has been automatically closed by the system, please read ==Use Maven Release Plugin directly from an existing tag== above.

#### Fix the error when close the staged repo

In the last step, you may see 6 errors similar to the following:
```
typeId	signature-staging
failureMessage	Invalid Signature: '/org/apache/sedona/sedona-python-adapter-2.4_2.12/1.0.1-incubating/sedona-python-adapter-2.4_2.12-1.0.1-incubating.pom.asc' is not a valid signature for 'sedona-python-adapter-2.4_2.12-1.0.1-incubating.pom'.
failureMessage	Invalid Signature: '/org/apache/sedona/sedona-python-adapter-2.4_2.11/1.0.1-incubating/sedona-python-adapter-2.4_2.11-1.0.1-incubating.pom.asc' is not a valid signature for 'sedona-python-adapter-2.4_2.11-1.0.1-incubating.pom'.
failureMessage	Invalid Signature: '/org/apache/sedona/sedona-python-adapter-3.0_2.12/1.0.1-incubating/sedona-python-adapter-3.0_2.12-1.0.1-incubating.pom.asc' is not a valid signature for 'sedona-python-adapter-3.0_2.12-1.0.1-incubating.pom'.
failureMessage	Invalid Signature: '/org/apache/sedona/sedona-viz-2.4_2.12/1.0.1-incubating/sedona-viz-2.4_2.12-1.0.1-incubating.pom.asc' is not a valid signature for 'sedona-viz-2.4_2.12-1.0.1-incubating.pom'.
failureMessage	Invalid Signature: '/org/apache/sedona/sedona-viz-3.0_2.12/1.0.1-incubating/sedona-viz-3.0_2.12-1.0.1-incubating.pom.asc' is not a valid signature for 'sedona-viz-3.0_2.12-1.0.1-incubating.pom'.
failureMessage	Invalid Signature: '/org/apache/sedona/sedona-viz-2.4_2.11/1.0.1-incubating/sedona-viz-2.4_2.11-1.0.1-incubating.pom.asc' is not a valid signature for 'sedona-viz-2.4_2.11-1.0.1-incubating.pom'.
```

This is caused by a bug in the resolved-pom-maven-plugin in POM.xml. You will have to upload the signatures of the 6 POM files mannualy. Please follow the steps below.

1. Please download the correct 6 pom files from the staging repo. On the web interface. Click Staging Repositories, select the latest Sedona staging repo, cick Content, find the aforementioned 6 pom files, download them to your local path.
2. Generate asc files for the 6 pom files. For example,
```
gpg -ab sedona-viz-3.0_2.12-1.0.1-incubating.pom
```
3. Delete the asc files of the 6 pom files. Follow the steps in Step 2, and delete them from the web interface.
4. Upload the 6 asc files to the staging repo using curl. For example,
```
curl -v -u admin:admin123 --upload-file sedona-python-adapter-2.4_2.11-1.0.1-incubating.pom.asc https://repository.apache.org/service/local/repositories/orgapachesedona-1010/content/org/apache/sedona/sedona-python-adapter-2.4_2.11/1.0.1-incubating/sedona-python-adapter-2.4_2.11-1.0.1-incubating.pom.asc

curl -v -u admin:admin123 --upload-file sedona-python-adapter-2.4_2.12-1.0.1-incubating.pom.asc https://repository.apache.org/service/local/repositories/orgapachesedona-1010/content/org/apache/sedona/sedona-python-adapter-2.4_2.12/1.0.1-incubating/sedona-python-adapter-2.4_2.12-1.0.1-incubating.pom.asc

curl -v -u admin:admin123 --upload-file sedona-python-adapter-3.0_2.12-1.0.1-incubating.pom.asc https://repository.apache.org/service/local/repositories/orgapachesedona-1010/content/org/apache/sedona/sedona-python-adapter-3.0_2.12/1.0.1-incubating/sedona-python-adapter-3.0_2.12-1.0.1-incubating.pom.asc

curl -v -u admin:admin123 --upload-file sedona-viz-2.4_2.11-1.0.1-incubating.pom.asc https://repository.apache.org/service/local/repositories/orgapachesedona-1010/content/org/apache/sedona/sedona-viz-2.4_2.11/1.0.1-incubating/sedona-viz-2.4_2.11-1.0.1-incubating.pom.asc

curl -v -u admin:admin123 --upload-file sedona-viz-2.4_2.12-1.0.1-incubating.pom.asc https://repository.apache.org/service/local/repositories/orgapachesedona-1010/content/org/apache/sedona/sedona-viz-2.4_2.12/1.0.1-incubating/sedona-viz-2.4_2.12-1.0.1-incubating.pom.asc

curl -v -u admin:admin123 --upload-file sedona-viz-3.0_2.12-1.0.1-incubating.pom.asc https://repository.apache.org/service/local/repositories/orgapachesedona-1010/content/org/apache/sedona/sedona-viz-3.0_2.12/1.0.1-incubating/sedona-viz-3.0_2.12-1.0.1-incubating.pom.asc
```
admin is your Apache ID username and admin123 is your Apache ID password. You can find the correct upload path from the web interface.
5. Once the staging repo is closed, click "Release" on the web interface.

