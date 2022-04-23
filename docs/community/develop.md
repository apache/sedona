# Develop Sedona

## Scala/Java developers

### IDE

We recommend Intellij IDEA with Scala plugin installed.

### Import the project

#### Choose `Open`

<img src="/image/ide-java-1.png"/>

#### Go to the Sedona root folder (not a submodule folder) and choose `open`

<img src="/image/ide-java-2.png" style="width:500px;"/>

#### The IDE might show errors

The IDE usually has trouble understanding the complex project structure in Sedona. 

<img src="/image/ide-java-4.png"/>

#### Fix errors by changing POM.xml

You need to comment out the following lines in `pom.xml` at the root folder, as follows. ==Remember that you should NOT submit this change to Sedona.==

```xml
<!--    <parent>-->
<!--        <groupId>org.apache</groupId>-->
<!--        <artifactId>apache</artifactId>-->
<!--        <version>23</version>-->
<!--        <relativePath />-->
<!--    </parent>-->
```

#### Reload POM.xml

Make sure you reload the POM.xml or reload the maven project. The IDE will ask you to remove some modules. Please select `yes`.

<img src="/image/ide-java-5.png"/>


#### The final project structure should be like this:

<img src="/image/ide-java-3.png" style="width:400px;"/>

### Run unit tests

#### Run all unit tests

In a terminal, go to the Sedona root folder. Run `mvn clean install`. All tests will take more than 15 minutes. To only build the project jars, run `mvn clean install -DskipTests`.

More details can be found on [Compile Sedona](/setup/compile/)

#### Run a single unit test

In the IDE, right-click a test case and run this test case.

<img src="/image/ide-java-6.png" style="width:400px;"/>

The IDE might tell you that the PATH does not exist as follows:

<img src="/image/ide-java-7.png" style="width:600px;"/>

Go to `Edit Configuration`

<img src="/image/ide-java-8.png"/>

Append the submodule folder to `Working Directory`. For example, `incubator-sedona/sql`.

<img src="/image/ide-java-9.png"/>

Re-run the test case. Do NOT right click the test case to re-run. Instead, click the button as shown in the figure below.

<img src="/image/ide-java-10.png"/>

## Python developers

More details to come.

### IDE

We recommend PyCharm

### Import the project

## R developers

More details to come.


### IDE

We recommend RStudio

### Import the project
