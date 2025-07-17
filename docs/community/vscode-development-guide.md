---
layout: docs
title: Apache Sedona VSCode Development Guide
permalink: /community/vscode-development-guide/
---

Setting up Apache Sedona for Development in VSCode
==================================================

This guide provides comprehensive instructions for setting up your development environment for Apache Sedona using Visual Studio Code, focusing on Scala/Java development.

Prerequisites
-------------

Ensure you have the following installed on your system:

* **Java Development Kit (JDK) 8:** Apache Sedona currently targets JDK 8.
    * **Installation:** Use your system's package manager (e.g., Homebrew on macOS: `brew install openjdk@8`) or download directly from Oracle/Adoptium.
    * **`JAVA_HOME`:** Set your `JAVA_HOME` environment variable to point to your JDK 8 installation. For example, in your shell profile (`~/.zshrc`, `~/.bashrc`):

        ```bash
        export JAVA_HOME="/path/to/your/jdk8" # e.g., /opt/homebrew/opt/openjdk@8
        export PATH="$JAVA_HOME/bin:$PATH"
        ```

* **Maven:** Apache Sedona uses Maven for dependency management and building.
    * **Installation:** Install Maven via your package manager (e.g., `brew install maven`) or download from the Apache Maven website.
* **Git:** For version control.

Recommended VSCode Extensions
-----------------------------

Install the following extensions in VSCode to enhance your Scala/Java development experience:

1. **Extension Pack for Java (by Microsoft):** This pack bundles essential Java extensions, including:
    * Language Support for Java™ by Red Hat
    * Debugger for Java
    * Maven for Java
    * and others.
    [Install from VSCode Marketplace](https://marketplace.visualstudio.com/items?itemName=vscjava.vscode-java-pack)

2. **Scala (Metals) (by Scala Metals):** Provides rich language support for Scala.
    [Install from VSCode Marketplace](https://marketplace.visualstudio.com/items?itemName=scalameta.metals)

Project Setup in VSCode
-----------------------

1. **Clone Your Fork:** If you haven't already, clone your fork of the Apache Sedona repository:

    ```bash
    git clone [https://github.com/YourGitHubUsername/sedona.git](https://github.com/YourGitHubUsername/sedona.git)
    cd sedona
    ```

    (Replace `YourGitHubUsername` with your actual GitHub username.)

2. **Open Project in VSCode:** Navigate your terminal into the `sedona` root directory and open the project:

    ```bash
    code .
    ```

3. **Maven Project Import:** Upon opening the folder, VSCode's "Maven for Java" extension should automatically detect the `pom.xml` files. You might see a prompt asking to import the Maven projects; accept this. If not, open the Maven sidebar (look for the Apache Maven icon, often an 'M' or an elephant head on the left activity bar), expand the `sedona` project, and ensure all modules are loaded. A refresh button might be available if projects are not detected.

    *Initial warnings about missing packages (e.g., `The import org.apache.sedona.sql.utils cannot be resolved`) are common at this stage. A full Maven build, as described below, usually resolves these by downloading all necessary dependencies.*

Building the Project
--------------------

You can build Apache Sedona using Maven directly from the VSCode integrated terminal.

1. Open the integrated terminal in VSCode (`Terminal > New Terminal`).
2. Run a full Maven build:

    ```bash
    mvn clean install -DskipTests
    ```

    * `mvn clean install`: Cleans previous builds, compiles the project, runs unit tests (if `-DskipTests` is not used), and installs artifacts to your local Maven repository.
    * `-DskipTests`: Skips running the unit tests, which significantly speeds up the build process for development purposes.

    For more detailed information on compiling Sedona, refer to the [Compile Sedona guide](https://sedona.apache.org/latest/community/develop/#run-all-unit-tests) in the main `Develop` documentation.

Running Tests
-------------

### Running Java Tests

VSCode's "Extension Pack for Java" provides excellent integration for running Java unit tests.

1. **From the editor:** Open any Java test file (e.g., `core/src/test/java/org/apache/sedona/core/formatMapper/EarthdataRasterReaderTest.java`). You will see "Run Test" and "Debug Test" buttons appearing above individual test methods and test classes. Click these to run or debug.
2. **Using the Test Explorer:**
    * Click the "Test" icon in the Activity Bar on the left (a beaker icon).
    * This view lists all discovered tests in your project. You can run all tests, tests within a specific module, or individual tests.

### Running Scala Tests

While the "Scala (Metals)" extension provides language support, direct integration into VSCode's Test Explorer for Scala tests might have limitations (as noted in GitHub Issue #1742). The most reliable way to run Scala tests is via Maven.

1. **Run all Scala tests for a module:**
    Open the integrated terminal and run Maven, specifying the module:

    ```bash
    # Example for the 'sql' module's Scala tests
    mvn test -pl sql # Adjust 'sql' to the relevant module
    ```

2. **Run a single Scala test file or test case via Maven (Recommended):**
    For more granular control, you can target specific Scala test classes or methods directly using Maven's `surefire` plugin (for ScalaTest, this typically works via `scalatest-maven-plugin` or `surefire` itself if ScalaTest is integrated).

    ```bash
    # Example: Run a specific Scala test class within a module
    mvn test -pl <module_name> -Dtest=<YourScalaTestClassName>

    # Example: Run a specific method within a Scala test class
    mvn test -pl <module_name> -Dtest=<YourScalaTestClassName>#<yourTestMethodName>
    ```

    (Replace `<module_name>`, `<YourScalaTestClassName>`, and `<yourTestMethodName>` with actual values from the Sedona codebase, e.g., `sql`, `TestPredicate`, `test_st_contains`.)

Addressing Common Issues
------------------------

### "The import org.apache.sedona.sql.utils cannot be resolved"

This type of error (missing package imports in the editor) often occurs because VSCode's Java Language Server hasn't fully picked up the project's compiled artifacts and dependencies.

* **Solution 1: Clean Build and Reload Workspace:**
    1. Perform a clean Maven build in the terminal:

        ```bash
        mvn clean install -DskipTests
        ```

    2. In VSCode, open the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P`), type "Java: Clean Java Language Server Workspace" and select the command. Then, restart VSCode if prompted.
* **Solution 2: Maven Update:**
    In the Maven sidebar, try clicking the "Reload All Maven Projects" button (often a refresh icon).

### `package sun.misc does not exist` (when using Java 11+)

If you encounter this error, particularly when running tests with a JDK higher than 8 (e.g., Java 11), it's due to how newer JDKs handle internal APIs.

* **Solution:** Follow the existing `Develop` guide's advice for IntelliJ: in your IDE settings, disable the option similar to "Use '--release' option for cross-compilation" for your Java compiler. The exact setting in VSCode might vary depending on your Java extension configuration, but generally, ensure the Java language server is configured to use JDK 8 explicitly for this project, or ensure appropriate JVM arguments are passed if running with a higher JDK (e.g., `--add-exports`).

Debugging
---------

### Java Debugging

1. **Set Breakpoints:** Click in the gutter (left margin next to line numbers) of your Java code to set breakpoints.
2. **Start Debugging:**
    * Click the "Run and Debug" icon in the Activity Bar (a triangle with a bug).
    * Click "Run and Debug" or use the "Debug Test" buttons mentioned in the Java Test section.
    * VSCode will automatically create a debug configuration.

### Scala Debugging

The "Scala (Metals)" extension supports debugging. You might need to create a `launch.json` configuration for specific debugging scenarios.

* **Steps (General):**
    1. Open the "Run and Debug" view.
    2. Click "create a launch.json file".
    3. Select "Metals" or "Java" as the environment.
    4. Configure the `launch.json` file for your specific test or application.
    * *For detailed Scala debugging setup with Metals, refer to the [Metals documentation](https://scalameta.org/metals/docs/editors/vscode/#run-and-debug).*