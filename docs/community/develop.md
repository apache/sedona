# Develop Sedona

## Scala/Java developers

### IDE

We recommend Intellij IDEA with Scala plugin installed. Please make sure that the IDE has JDK 1.8 set as project default.

### Import the project

#### Choose `Open`

![](../image/ide-java-1.png)

#### Go to the Sedona root folder (not a submodule folder) and choose `open`

![](../image/ide-java-2.png){: width="500px"}

#### The IDE might show errors

The IDE usually has trouble understanding the complex project structure in Sedona.

![](../image/ide-java-4.png)

#### Fix errors by changing pom.xml

You need to comment out the following lines in `pom.xml` at the root folder, as follows. ==Remember that you should NOT submit this change to Sedona.==

```xml
<!--    <parent>-->
<!--        <groupId>org.apache</groupId>-->
<!--        <artifactId>apache</artifactId>-->
<!--        <version>23</version>-->
<!--        <relativePath />-->
<!--    </parent>-->
```

#### Reload pom.xml

Make sure you reload the pom.xml or reload the maven project. The IDE will ask you to remove some modules. Please select `yes`.

![](../image/ide-java-5.png)

#### The final project structure should be like this:

![](../image/ide-java-3.png){: width="400px"}

### Run unit tests

#### Run all unit tests

In a terminal, go to the Sedona root folder. Run `mvn clean install`. All tests will take more than 15 minutes. To only build the project jars, run `mvn clean install -DskipTests`.
!!!Note
    `mvn clean install` will compile Sedona with Spark 3.0 and Scala 2.12. If you have a different version of Spark in $SPARK_HOME, make sure to specify that using -Dspark command line arg.
    For example, to compile sedona with Spark 3.4 and Scala 2.12, use: `mvn clean install -Dspark=3.4 -Dscala=2.12`

More details can be found on [Compile Sedona](../setup/compile.md)

#### Run a single unit test

In the IDE, right-click a test case and run this test case.

![](../image/ide-java-6.png){: width="400px"}

The IDE might tell you that the PATH does not exist as follows:

![](../image/ide-java-7.png){: width="600px"}

Go to `Edit Configuration`

![](../image/ide-java-8.png)

Append the submodule folder to `Working Directory`. For example, `sedona/sql`.

![](../image/ide-java-9.png)

Re-run the test case. Do NOT right-click the test case to re-run. Instead, click the button as shown in the figure below.

![](../image/ide-java-10.png)

## Python developers

#### Run all python tests

To run all Python test cases, follow steps mentioned [here](../setup/compile.md#run-python-test).

#### Run all python tests in a single test file

To run a particular python test file, specify the path of the .py file to pipenv.

For example, to run all tests in `test_function.py` located in `python/tests/sql/`, use: `pipenv run pytest tests/sql/test_function.py`.

#### Run a single test

To run a particular test in a particular .py test file, specify `file_name::class_name::test_name` to the pytest command.

For example, to run the test on ST_Contains function located in sql/test_predicate.py, use: `pipenv run pytest tests/sql/test_predicate.py::TestPredicate::test_st_contains`

### IDE

We recommend PyCharm

### Import the project

## R developers

More details to come.

### IDE

We recommend RStudio

### Import the project
