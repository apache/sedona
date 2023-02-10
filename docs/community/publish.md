# Make a Sedona release

This page is for Sedona PMC to publish Sedona releases.

!!!warning
    All scripts on this page should be run in your local Sedona Git repo under master branch via a single script file.

## 0. Prepare an empty script file

1. In your local Sedona Git repo under master branch, run
```bash
echo "#!/bin/bash" > create-release.sh
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
wget -q https://dlcdn.apache.org//creadur/apache-rat-$RAT_VERSION/apache-rat-0.15-bin.tar.gz
tar -xvf  apache-rat-0.15-bin.tar.gz
git clone --shared --branch master https://github.com/apache/sedona.git sedona-src
java -jar apache-rat-0.15.jar -d sedona-src > report.txt
```
2. Read the generated report.txt file and make sure all source code files have ASF header.
3. Delete the generated report and cloned files
```bash
#!/bin/bash
rm -rf sedona-src
rm report.txt
```

## 2. Update Sedona Python, R and Zeppelin versions

Make sure the Sedona version in the following files are {{ sedona_create_release.current_version }}. 

1. https://github.com/apache/sedona/blob/master/python/sedona/version.py
2. https://github.com/apache/sedona/blob/master/R/DESCRIPTION
3. https://github.com/apache/sedona/blob/master/zeppelin/package.json


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

source ~/.bashrc

git checkout master
git pull

rm -f release.*
rm -f pom.xml.*

echo "*****Step 1. Stage the Release Candidate to GitHub."

mvn -q -B clean release:prepare -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.current_snapshot }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests"

rm -f release.*
rm -f pom.xml.*

echo "Now the releases are staged. A tag and two commits have been created on Sedona GitHub repo"

echo "*****Step 2: Upload the Release Candidate to https://repository.apache.org."

# For Spark 3.0 and Scala 2.12
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.12"

# For Spark 3.0 and Scala 2.13
mvn -q org.apache.maven.plugins:maven-release-plugin:2.3.2:perform -DconnectionUrl=scm:git:https://github.com/apache/sedona.git -Dtag={{ sedona_create_release.current_git_tag }} -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.13"

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

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dscala=2.12 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/core/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/sql/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/viz/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/python-adapter/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/flink/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

cd apache-sedona-{{ sedona_create_release.current_version }}-src && mvn -q clean install -DskipTests -Dscala=2.13 && cd ..
cp apache-sedona-{{ sedona_create_release.current_version }}-src/core/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/sql/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/viz/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/python-adapter/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/
cp apache-sedona-{{ sedona_create_release.current_version }}-src/flink/target/sedona-*{{ sedona_create_release.current_version}}.jar apache-sedona-{{ sedona_create_release.current_version }}-bin/

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

Instruction for checking items on the checklist: https://sedona.apache.org/community/vote/

We recommend you use this Jupyter notebook on MyBinder to perform this task: https://mybinder.org/v2/gh/jiayuasu/sedona-tools/HEAD?labpath=binder%2Fverify-release.ipynb

**Please vote accordingly and you must provide your checklist for your vote**.


[ ] +1 approve

[ ] +0 no opinion

[ ] -1 disapprove with the reason

Checklist:

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

echo "Re-staging releases to https://repository.apache.org"

# For Spark 3.0 and Scala 2.12

git pull

git checkout -b {{ sedona_create_release.current_git_tag }} {{ sedona_create_release.current_git_tag }}

mvn versions:set -DnewVersion={{ sedona_create_release.current_version }}-SNAPSHOT

git add -A

git commit -m "tmp SNAPSHOT pom"

mvn clean release:prepare -DautoVersionSubmodules=true -DdryRun=true -Dresume=false -Darguments="-DskipTests" -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.next_version }}

mvn versions:set -DnewVersion={{ sedona_create_release.current_version }}

git add -A

git commit -m "revert back to the correct pom"

mvn clean release:perform -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests"

rm -f release.*
rm -f pom.xml.*

# For Spark 3.0 and Scala 2.13

mvn versions:set -DnewVersion={{ sedona_create_release.current_version }}-SNAPSHOT

git add -A

git commit -m "tmp SNAPSHOT pom"

mvn clean release:prepare -DautoVersionSubmodules=true -DdryRun=true -Dresume=false -Darguments="-DskipTests -Dscala=2.13" -Dtag={{ sedona_create_release.current_git_tag }} -DreleaseVersion={{ sedona_create_release.current_version }} -DdevelopmentVersion={{ sedona_create_release.next_version }}

mvn versions:set -DnewVersion={{ sedona_create_release.current_version }}

git add -A

git commit -m "revert back to the correct pom"

mvn clean release:perform -DautoVersionSubmodules=true -Dresume=false -Darguments="-DskipTests -Dscala=2.13"

rm -f release.*
rm -f pom.xml.*

git add -A

git commit -m "cleanup the branch"

git checkout master

git branch -d {{ sedona_create_release.current_git_tag }}
```

### Fix signature issues

Please find the Sedona staging id on https://repository.apache.org under `staging repository`.

Then run the following script. Replace `admin`, `admind123` with your Apache ID username and Apache ID password. Replace `stagingid` with the correct id.

```bash
#!/bin/bash
username=admin
password=admin123
stagingid=1027

artifacts=(parent core-3.0_2.12 core-3.0_2.13 sql-3.0_2.12 sql-3.0_2.13 viz-3.0_2.12 viz-3.0_2.13 python-adapter-3.0_2.12 python-adapter-3.0_2.13 common flink_2.12)
filenames=(.pom .jar -javadoc.jar)

echo "Re-uploading signatures to fix *failureMessage Invalid Signature*"
for artifact in "${artifacts[@]}"; do
	for filename in "${filenames[@]}"; do
	if [ $artifact -eq 'parent' && $filename -ne '.pom' ]
    then
       continue
    fi
	wget https://repository.apache.org/service/local/repositories/orgapachesedona-$stagingid/content/org/apache/sedona/sedona-$artifact/{{ sedona_create_release.current_version }}/sedona-${artifact}-{{ sedona_create_release.current_version }}${filename}
	gpg -ab sedona-${artifact}-{{ sedona_create_release.current_version }}${filename}
	curl -v -u $username:$password --upload-file sedona-${artifact}-{{ sedona_create_release.current_version }}${filename}.asc https://repository.apache.org/service/local/repositories/orgapachesedona-$stagingid/content/org/apache/sedona/sedona-${artifact}/{{ sedona_create_release.current_version }}/sedona-${artifact}-{{ sedona_create_release.current_version }}${filename}.asc
   done
done

rm *.pom
rm *.jar
rm *.asc
```

### Manually close and release the package

1. Click `Close` on the Sedona staging repo on https://repository.apache.org under `staging repository`
2. Once the staging repo is closed, click `Release` on this repo.


## 9. Release Sedona Python and Zeppelin

You must have the maintainer privilege of `https://pypi.org/project/apache-sedona/` and `https://www.npmjs.com/package/apache-sedona`

```bash
#!/bin/bash

wget https://github.com/apache/sedona/archive/refs/tags/{{ sedona_create_release.current_git_tag}}.tar.gz
tar -xvf {{ sedona_create_release.current_git_tag}}.tar.gz
mkdir apache-sedona-{{ sedona_create_release.current_version }}-src
cp -r sedona-{{ sedona_create_release.current_git_tag}}/* apache-sedona-{{ sedona_create_release.current_version }}-src/

rm -rf sedona-{{ sedona_create_release.current_git_tag}}

cd apache-sedona-{{ sedona_create_release.current_version }}-src/python && python3 setup.py sdist bdist_wheel && twine upload dist/* && cd ..
cd zeppelin && npm publish && cd ..
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

### Prepare the environment and doc folder

1. Check out the {{ sedona_create_release.current_version }} Git tag on your local repo.
2. Read [Compile documentation website](../../setup/compile) to set up your environment. But don't deploy anything yet.
3. Add the download link to [Download page](../../download).
4. Add the news to `docs/index.md`.

### Generate Javadoc and Scaladoc

* Javadoc: Use Intelij IDEA to generate Javadoc for `core` and `viz` module, output them to `docs/api/javadoc`
* Scaladoc: Run `scaladoc -d docs/api/javadoc/sql/ sql/src/main/scala/org/apache/sedona/sql/utils/*.scala`

Please do not commit these generated docs to Sedona GitHub.

### Compile R html docs

1. Make sure you install R, tree and curl on your Ubuntu machine. On Mac, just do `brew install tree`
```
sudo apt install littler tree libcurl4-openssl-dev
```
2. In the Sedona root directory, run the script below. This will create `rdocs` folder in Sedona `/docs/api/rdocs`
```bash
#!/bin/bash
Rscript generate-docs.R
cd ./docs/api/rdocs && tree -H '.' -L 1 --noreport --charset utf-8 -o index.html && cd ../../../
```

### Deploy the website

1. Run `mike deploy --push --update-aliases {{ sedona_create_release.current_version }} latest`. This will deploy this website to Sedona main repo's gh-page. But Sedona does not use gh-page for hosting website.
2. Check out the master branch.
3. Git commit and push your changes in `download.md` and `index.md` to master branch. Delete all generated docs.
4. Check out the `gh-page` branch.
5. In a separate folder, check out GitHub sedona-website [asf-site branch](https://github.com/apache/sedona-website/tree/asf-site)
6. Copy all content to in Sedona main repo `gh-page` branch to Sedona website repo `asf-site` branch.
7. Commit and push the changes to the remote `asf-site` branch.