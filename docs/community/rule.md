# Contributing to Apache Sedona

The project welcomes contributions. You can contribute to Sedona code or documentation by making Pull Requests on [Sedona GitHub Repo](https://github.com/apache/incubator-sedona).


The following sections brief the workflow of how to complete a contribution.

## Pick / Annouce a task

It is important to confirm that your contribution is acceptable. You should create a JIRA ticket or send an email to `dev@sedona.apache.org`


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
	Please read [Compile the source code](../download/compile.md#compile-the-documentation) to learn how to compile Sedona website.

## Create a JIRA ticket and make a Pull Request
After developing a contribution, the easiest and most visible way to push a contribution is to create a JIRA ticket and submit a Pull Request (PR) to the [GitHub repo](https://github.com/apache/incubator-sedona). Please use the JIRA ticket ID in the PR name, such as "[SEDONA-1] my subject".

When preparing a PR, please answser the following questions in the PR:

1.  What changes were proposed in this pull request?

2. How was this patch tested?

When a PR is submitted Travis CI will check the build correctness. Please check the PR status, and fix any reported problems.