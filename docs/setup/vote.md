# Vote a Sedona release

This page is for Sedona community to vote a Sedona release. The script below is tested on MacOS.

In order to vote a Sedona release, you must provide your checklist inlcuding the following minimum requirement:

* Download links are valid
* Checksums and PGP signatures are valid
* DISCLAIMER and NOTICE are included
* Source code artifacts have correct names matching the current release
* The project can compile from the source code

If you can successfully finish the steps below, you will pass the items mentioned above. Then you can vote `+1` in the vote email and provide your checklist.

## Install necessary software

1. GPG: On Mac `brew install gnupg gnupg2`. You can check in a terminal `gpg --version`.
2. JDK 1.8 or 1.11. Your Mac might have many different Java versions installed. You can try to use it but not sure if it can pass. You can check in a terminal `java --version`.
3. Apache Maven 3.3.1+. On Mac `brew install maven`. You can check it in a terminal `mvn -version`.
4. Python3 installed on your machine. MacOS comes with Python3 by default. You can check in a terminal `python3 --version`.

You can skip this step if you installed these software before.


## Download a Sedona release

```bash
#!/bin/bash
wget https://downloads.apache.org/incubator/sedona/KEYS
wget https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-src.tar.gz
wget https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-src.tar.gz.asc
wget https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-src.tar.gz.sha512
wget https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-bin.tar.gz
wget https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-bin.tar.gz.asc
wget https://dist.apache.org/repos/dist/dev/incubator/sedona/{{ sedona.current_rc }}/apache-sedona-{{ sedona.current_version }}-bin.tar.gz.sha512
```

## Verify the signature and checksum

```bash
#!/bin/bash
gpg --import KEYS.txt
gpg --verify apache-sedona-{{ sedona.current_version }}-src.tar.gz.asc
gpg --verify apache-sedona-{{ sedona.current_version }}-bin.tar.gz.asc
shasum -a 512 apache-sedona-{{ sedona.current_version }}-src.tar.gz
cat apache-sedona-{{ sedona.current_version }}-src.tar.gz.sha512
shasum -a 512 apache-sedona-{{ sedona.current_version }}-bin.tar.gz
cat apache-sedona-{{ sedona.current_version }}-bin.tar.gz.sha512
```

If successful, you should be able to see something similar to the following text. It should include `Good signature from` and the final 4 lines should be two pairs of checksum matching each other.

```
gpg: key 3A79A47AC26FF4CD: "Jia Yu <jiayu@apache.org>" not changed
gpg: key 6C883CA80E7FD299: "PawelKocinski <imbruced@apache.org>" not changed
gpg: Total number processed: 2
gpg:              unchanged: 2
gpg: assuming signed data in 'apache-sedona-1.2.0-incubating-src.tar.gz'
gpg: Signature made Mon Apr  4 11:48:31 2022 PDT
gpg:                using RSA key 949DD6275C69AB954B1872FC6C883CA80E7FD299
gpg:                issuer "imbruced@apache.org"
gpg: Good signature from "PawelKocinski <imbruced@apache.org>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 949D D627 5C69 AB95 4B18  72FC 6C88 3CA8 0E7F D299
gpg: assuming signed data in 'apache-sedona-1.2.0-incubating-bin.tar.gz'
gpg: Signature made Mon Apr  4 11:48:42 2022 PDT
gpg:                using RSA key 949DD6275C69AB954B1872FC6C883CA80E7FD299
gpg:                issuer "imbruced@apache.org"
gpg: Good signature from "PawelKocinski <imbruced@apache.org>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 949D D627 5C69 AB95 4B18  72FC 6C88 3CA8 0E7F D299
d3bdfd4d870838ebe63f21cb93634d2421ec1ac1b8184636206a5dc0d89a78a88257798b1f17371ad3cfcc3b1eb79c69e1410afdefeb4d9b52fc8bb5ea18dd2e  apache-sedona-1.2.0-incubating-src.tar.gz
d3bdfd4d870838ebe63f21cb93634d2421ec1ac1b8184636206a5dc0d89a78a88257798b1f17371ad3cfcc3b1eb79c69e1410afdefeb4d9b52fc8bb5ea18dd2e  apache-sedona-1.2.0-incubating-src.tar.gz
64cea38dd3ca171ee4e2a7365dbce999773862f2a11599bd0f27e9551d740659a519a9b976b3e7b0826088010967093e6acc9462f7073e9737c24b007a2df846  apache-sedona-1.2.0-incubating-bin.tar.gz
64cea38dd3ca171ee4e2a7365dbce999773862f2a11599bd0f27e9551d740659a519a9b976b3e7b0826088010967093e6acc9462f7073e9737c24b007a2df846  apache-sedona-1.2.0-incubating-bin.tar.gz
```

## Check files manually

1. Check if the downloaded files have the correct version: `{{ sedona.current_version }}`
 
2. Unzip the downloaded `apache-sedona-{{ sedona.current_version }}-src.tar.gz` file, and check if DISCLAIMER and NOTICE files and included and up to date.

## Compile the project from source

In the unzipped source code folder, try to compile the source code

```
mvn clean install -DskipTests
```

If everything works fine, this should give you `Build Success`. If this step fails, you can contact Sedona PPMC and see if this is just because of your environment.

