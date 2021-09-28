Sedona binary releases are compiled by Java 1.8 and Scala 2.11/2.12 and tested in the following environments:

=== "Sedona Scala/Java"
	
	|             | Spark 2.4 | Spark 3.0 | Spark 3.1 |
	|:-----------:| :---------:|:---------:|:---------:|
	| Scala 2.11  |  ✅  |  not tested  | not tested  |
	| Scala 2.12 | ✅  |  ✅  | ✅ |

=== "Sedona Python"
	
	|             | Spark 2.4 (Scala 2.11) | Spark 3.0 (Scala 2.12)|Spark 3.1 (Scala 2.12)|
	|:-----------:|:---------:|:---------:|:---------:|
	| Python 3.7  |  ✅  |  ✅  |  ✅  |
	| Python 3.8 | not tested  |  not tested  |  ✅  |
	| Python 3.9 | not tested  |  not tested  |  ✅  |

=== "Sedona R"
	
	|             | Spark 2.4 | Spark 3.0 | Spark 3.1 |
	|:-----------:| :---------:|:---------:|:---------:|
	| Scala 2.11  |  ✅  |  not tested  | not tested  |
	| Scala 2.12 | not tested  |  ✅  | ✅ |
	
!!!warning
	Sedona Scala/Java/Python/R also work with Spark 2.3, Python 3.6 but we have no plan to officially support it.