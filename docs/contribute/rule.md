# Contributing to GeoSpark

The project welcomes contributions. You can contribute to GeoSpark code or documentation by making Pull Requests on [GeoSpark GitHub Repo](https://github.com/DataSystemsLab/GeoSpark).


The following sections brief the workflow of how to complete a contribution.

## Pick / Annouce a task

It is important to confirm that your contribution is acceptable. Hence, you have two options to start with:

* Pick an issue from the Issues tagged by ==Help Wanted== on [GeoSpark Issues](https://github.com/DataSystemsLab/GeoSpark/issues).

* Announce what you are going to work on in advance if no GitHub issues match your scenario. To do this, contact [GeoSpark project committer](../contact/contact.md#committer).



## Develop a code contribution

Code contributions should include the following:

* Detailed documentations on classes and methods.
* Unit Tests to demonstrate code correctness and allow this to be maintained going forward.  In the case of bug fixes the unit test should demonstrate the bug in the absence of the fix (if any).  Unit Tests can be JUnit test or Scala test. Some GeoSpark functions need to be tested in both Scala and Java.
* Updates on corresponding GeoSpark documentation if necessary.

Code contributions must include a license header at the top of each file.  A sample header for Scala/Java files is as follows:
```
/*
 * FILE: SpatialRDD
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
```

## Develop a document contribution
Documentation contributions should satisfy the following requirements:

* Detailed explanation with examples.
* Place a newly added document in a proper folder
* Change the ==mkdocs.yml== if necessary

!!!note
	Please read [Compile the source code](../download/compile.md#compile-the-documentation) to learn how to compile GeoSpark website.

## Make a Pull Request
After developing a contribution, the easiest and most visible way to push a contribution is to submit a Pull Request (PR) to the [GitHub repo](https://github.com/DataSystemsLab/GeoSpark).  

When preparing a PR, please answser the following questions in the PR:

1.  What changes were proposed in this pull request?

2. How was this patch tested?

When a PR is submitted Travis CI will check the build correctness. Please check the PR status, and fix any reported problems.