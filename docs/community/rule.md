# Contributing to Apache Sedona

The project welcomes contributions. You can contribute to Sedona code or documentation by making Pull Requests on [Sedona GitHub Repo](https://github.com/apache/sedona).


The following sections brief the workflow of how to complete a contribution.

## Pick / Announce a task using JIRA

It is important to confirm that your contribution is acceptable. You should create a JIRA ticket or pick an existing ticket. A new JIRA ticket will be automatically sent to `dev@sedona.apache.org`


## Develop a code contribution

Code contributions should include the following:

* Detailed documentations on classes and methods.
* Unit Tests to demonstrate code correctness and allow this to be maintained going forward.  In the case of bug fixes the unit test should demonstrate the bug in the absence of the fix (if any).  Unit Tests can be JUnit test or Scala test. Some Sedona functions need to be tested in both Scala and Java.
* Updates on corresponding Sedona documentation if necessary.

Code contributions must include a Apache 2.0 license header at the top of each file.

## Develop a document contribution

Documentation contributions should satisfy the following requirements:

* Detailed explanation with examples.
* Place a newly added document in a proper folder
* Change the ==mkdocs.yml== if necessary

!!!note
	Please read [Compile the source code](../setup/compile.md#compile-the-documentation) to learn how to compile Sedona website.

## Make a Pull Request

After developing a contribution, the easiest and most visible way to submit a Pull Request (PR) to the [GitHub repo](https://github.com/apache/sedona). 

**Please use the JIRA ticket ID in the PR name, such as "[SEDONA-1] my subject".**

When creating a PR, please answser the questions in the PR template.

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