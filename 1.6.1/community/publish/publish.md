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

mvn -q -B clean release:prepare -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.current_snapshot }} -Dresume=false -Penable-all-submodules -Darguments="-DskipTests"
mvn -q -B release:clean -Penable-all-submodules

echo "Now the releases are staged. A tag and two commits have been created on Sedona GitHub repo"

echo "*****Step 2: Upload the Release Candidate to https://repository.apache.org."

# For Spark 3.0 and Scala 2.12
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.0 -Dscala=2.12" -Dspark=3.0 -Dscala=2.12

# For Spark 3.0 and Scala 2.13
## Note that we use maven-release-plugin 2.3.2 instead of more recent version (e.g., 3.0.1) to get rid of a bug of maven-release-plugin,
## which prevent us from cloning git repo with user specified -Dtag=<tag>.
## Please refer to https://issues.apache.org/jira/browse/MRELEASE-933 and https://issues.apache.org/jira/browse/SCM-729 for details.
##
## Please also note that system properties `-Dspark` and `-Dscala` has to be specified both for release:perform and the actual build parameters
## in `-Darguments`, because the build profiles activated for release:perform task will also affect the actual build task. It is safer to specify
## these system properties for both tasks.
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.0 -Dscala=2.13" -Dspark=3.0 -Dscala=2.13

# For Spark 3.4 and Scala 2.12
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.4 -Dscala=2.12" -Dspark=3.4 -Dscala=2.12

# For Spark 3.4 and Scala 2.13
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.4 -Dscala=2.13" -Dspark=3.4 -Dscala=2.13

# For Spark 3.5 and Scala 2.12
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.5 -Dscala=2.12" -Dspark=3.4 -Dscala=2.12

# For Spark 3.5 and Scala 2.13
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.5 -Dscala=2.13" -Dspark=3.4 -Dscala=2.13

echo "*****Step 3: Upload Release Candidate on ASF SVN: https://dist.apache.org/repos/dist/dev/sedona"

echo "Creating {{ sedona_create_release.current_rc }} folder on SVN..."

svn mkdir -m "Adding folder" https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}

echo "Creating release files locally..."

echo "Downloading source code..."

wget https://github.com/apache/sedona/archive/refs/tags/{{ sedona_create_release.current_git_tag}}.tar.gz
tar -xvf {{ sedona_create_release.current_git_tag}}.tar.gz
mkdir apache-sedona-{{ sedona_create_release.current_version }}-src
cp -r sedona-{{ sedona_create_release.current_git_tag}}/* apache-sedona-{{ sedona_create_release.current_version }}-src/
tar czf apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz apache-sedona-{{ sedona_create_release.current_version }}-src
rm {{ sedona_create_release.current_git_tag}}.tar.gz
rm -rf sedona-{{ sedona_create_release.current_git_tag}}

echo "Compiling the source code..."

mkdir apache-sedona-{{ sedona_create_release.current_version }}-bin

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dspark=3.0 -Dscala=2.12 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/spark-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/flink-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/snowflake/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dspark=3.0 -Dscala=2.13 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/spark-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dspark=3.4 -Dscala=2.12 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/spark-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dspark=3.4 -Dscala=2.13 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/spark-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dspark=3.5 -Dscala=2.12 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/spark-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dspark=3.5 -Dscala=2.13 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/spark-shaded/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

tar czf apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz apache-sedona-{{ sedona_create_release.current_version }}-bin
shasum -a 512 apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz > apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512
shasum -a 512 apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz > apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512
gpg -ab apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz
gpg -ab apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz

echo "Uploading local release files..."

svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc
svn import -m "Adding file" apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512 https://dist.apache.org/repos/dist/dev/sedona/{{ sedona_create_release.current_rc }}/apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512

echo "Removing local release files..."

rm apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz
rm apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.asc
rm apache-sedona-{{ sedona_create_release.current_version }}-src.tar.gz.sha512
rm apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz
rm apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.asc
rm apache-sedona-{{ sedona_create_release.current_version }}-bin.tar.gz.sha512
rm -rf apache-sedona-{{ sedona_create_release.current_version }}-src
rm -rf apache-sedona-{{ sedona_create_release.current_version }}-bin

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

**NOTICE**: The staging repo will be automatically dropped after 3 days without closing. If you find the staging repo being dropped, you can re-stage the release using the following script.

```bash
#!/bin/bash

echo "Re-staging releases to https://repository.apache.org"

git checkout master
git pull

rm -f release.*
rm -f pom.xml.*

# For Spark 3.0 and Scala 2.12
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.0 -Dscala=2.12" -Dspark=3.0 -Dscala=2.12

# For Spark 3.0 and Scala 2.13
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.0 -Dscala=2.13" -Dspark=3.0 -Dscala=2.13

# For Spark 3.4 and Scala 2.12
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.4 -Dscala=2.12" -Dspark=3.4 -Dscala=2.12

# For Spark 3.4 and Scala 2.13
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -Dresume=false -Darguments="-DskipTests -Dspark=3.4 -Dscala=2.13" -Dspark=3.4 -Dscala=2.13
```

## 9. Release Sedona Python and Zeppelin

You must have the maintainer privilege of `https://pypi.org/project/apache-sedona/` and `https://www.npmjs.com/package/apache-sedona`

To publish Sedona pythons, you have to use GitHub actions since we release wheels for different platforms. Please use this repo: https://github.com/jiayuasu/sedona-publish-python

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

## 10. Release Sedona R to CRAN.

```bash
#!/bin/bash
R CMD build .
R CMD check --as-cran apache.sedona_*.tar.gz
```

Then submit to CRAN using this [web form](https://xmpalantir.wu.ac.at/cransubmit/).

## 11. Publish the doc website

1. Check out the {{ sedona_create_release.current_version }} Git tag on your local repo to a branch namely `branch-{{ sedona_create_release.current_version }}`
2. Add the download link to [Download page](../download.md).
3. Add the news to `docs/index.md`.
4. Push the changes to this branch on GitHub.
5. GitHub CI will pick up the changes and deploy to `website` branch.
6. Normally [this repo](https://github.com/jiayuasu/sedona-sync-action) will automatically publish the website on a daily basis.
