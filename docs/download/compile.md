# Compile GeoSpark
Some GeoSpark hackers may want to change some source code to fit in their own scenarios. To compile GeoSpark source code, you first need to download GeoSpark source code:

* Download / Git clone GeoSpark source code from [GeoSpark Github repo](https://github.com/DataSystemsLab/GeoSpark).


## Compile the source code
GeoSpark is a a project with three modules, core, sql, and viz. Each module is a Scala/Java mixed project which is managed by Apache Maven 3. 

* Make sure your machine has Java 1.8 and Apache Maven 3.

To compile all modules, please make sure you are in the root folder of three modules. Then enter the following command in the terminal:

```
mvn clean install -DskipTests
```
This command will first delete the old binary files and compile all three modules. This compilation will skip the unit tests of GeoSpark.

To compile a module of GeoSpark, please make sure you are in the folder of that module. Then enter the same command.

To run unit tests, just simply remove `-DskipTests` option. The command is like this:
```
mvn clean install
```

!!!warning
	The unit tests of all three modules may take up to 30 minutes. 

## Compile the documentation
The source code of GeoSpark documentation website is written in Markdown and then compiled by MkDocs. The website is built upon [Material for MkDocs template](https://squidfunk.github.io/mkdocs-material/).

In GeoSpark repository, MkDocs configuration file ==mkdocs.yml== is in the root folder and all documentation source code is in docs folder.

To compile the source code and test the website on your local machine, please read [MkDocs Tutorial](http://www.mkdocs.org/#installation) and [Materials for MkDocs Tutorial](https://squidfunk.github.io/mkdocs-material/getting-started/).

After installing MkDocs and MkDocs-Material, run the command in GeoSpark root folder:

```
mkdocs serve
```