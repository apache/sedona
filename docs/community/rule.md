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

# Contributing to Apache Sedona

The project welcomes contributions. You can contribute to Sedona code or documentation by making Pull Requests on [Sedona GitHub Repo](https://github.com/apache/sedona).

The following sections brief the workflow of how to complete a contribution.

## Pick / Announce a task

It is important to confirm that your contribution is acceptable. Before starting a contribution, you should announce your intention to the community via tickets. Sedona allows tickets from both [GitHub issues](https://github.com/apache/sedona/issues?q=sort%3Aupdated-desc+is%3Aissue+is%3Aopen) and [JIRA](https://issues.apache.org/jira/projects/SEDONA). A new JIRA ticket will be automatically sent to `dev@sedona.apache.org`

## Develop a code contribution

Code contributions should include the following:

* Detailed documentations on classes and methods.
* Unit Tests to demonstrate code correctness and allow this to be maintained going forward. In the case of bug fixes the unit test should demonstrate the bug in the absence of the fix (if any). Unit Tests can be JUnit test or Scala test. Some Sedona functions need to be tested in both Scala and Java.
* Updates on corresponding Sedona documentation if necessary.

Code contributions must include an Apache 2.0 license header at the top of each file.

Please run `mvn spotless:apply` to format the code before making a pull request. If you've modified code for a specific spark version (for example, source files in spark/spark-3.5/), please add additional Maven CLI arguments to format that code: `mvn spotless:apply -Dscala=2.12 -Dspark=3.5`.

## Develop a document contribution

Documentation contributions should satisfy the following requirements:

* Detailed explanation with examples.
* Place a newly added document in a proper folder
* Change the ==mkdocs.yml== if necessary

!!!note
	Please read [Compile the source code](../setup/compile.md#compile-the-documentation) to learn how to compile Sedona website.

## Make a Pull Request

After developing a contribution, the easiest and most visible way to submit a Pull Request (PR) to the [GitHub repo](https://github.com/apache/sedona).

**Please use the JIRA ticket ID or GitHub Issue ID in the PR name, such as "[SEDONA-1] my subject" or ""[GH-1] my subject".**

When creating a PR, please answer the questions in the PR template.

When a PR is submitted, GitHub Action will check the build correctness. Please check the PR status, and fix any reported problems.

## Review a Pull Request

* Every PR requires (1) at least 1 approval from a committer and (2) no disapproval from a committer. Everyone is welcome to review a PR but only the committer can make the final decision.
* Other reviewers, including community members and committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch.
* Lively, polite, rapid technical debate is encouraged from everyone in the community even if the outcome may be a rejection of the entire change.
* Keep in mind that changes to more critical parts of Sedona, like Sedona core and spatial join algorithms, will be subjected to more review, and may require more testing and proof of its correctness than other changes.
* Sometimes, other changes will be merged which conflict with your pull request’s changes. The PR can’t be merged until the conflict is resolved. This can be resolved by resolving the conflicts by hand, then pushing the result to your branch.

## Code of Conduct

Please read [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

We expect everyone who participates in the Apache community formally or informally, or claims any affiliation with the Foundation, in any Foundation-related activities and especially when representing the ASF in any role to honor this code of conduct.
