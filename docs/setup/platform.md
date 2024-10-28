Sedona binary releases are compiled by Java 1.8 and Scala 2.11/2.12 and tested in the following environments:

!!!warning
	Support of Spark 3.0, 3.1, 3.2 was removed in Sedona 1.7.0+ although some parts of the source code might still be compatible.

=== "Sedona Scala/Java"

	|             | Spark 3.0 | Spark 3.1 | Spark 3.2| Spark 3.3| Spark 3.4| Spark 3.5 |
	|:---------:|:---------:|:---------:|:---------:|:---------:|:---------:|:---------:|
	| Scala 2.11  |  not tested  | not tested  | not tested  |not tested  |not tested |not tested |
	| Scala 2.12 | not tested  | not tested | not tested |✅ |✅ |✅ |
	| Scala 2.13 |  not tested  | not tested  | not tested|✅ |✅ |✅ |

=== "Sedona Python"

    |             | Spark 3.0 (Scala 2.12)|Spark 3.1 (Scala 2.12)| Spark 3.2 (Scala 2.12)| Spark 3.3 (Scala 2.12)|Spark 3.4 (Scala 2.12)|Spark 3.5 (Scala 2.12)|
    |:---------:|:---------:|:---------:|:---------:|:---------:|:---------:|:---------:|
    | Python 3.7  |  not tested  |  not tested  |  not tested  |  ✅  |  ✅  | ✅ |
    | Python 3.8 | not tested  |not tested  |not tested  |  ✅  |  ✅  | ✅  |
    | Python 3.9 | not tested  |not tested  |not tested  |  ✅  |  ✅  | ✅  |
    | Python 3.10 | not tested  |not tested  |not tested  |  ✅  |  ✅  | ✅  |

=== "Sedona R"

	|             | Spark 3.0 | Spark 3.1 | Spark 3.2 | Spark 3.3 | Spark 3.4 | Spark 3.5 |
	|:---------:|:---------:|:---------:|:---------:|:---------:|:---------:|:---------:|
	| Scala 2.11  |  not tested  | not tested  | not tested  | not tested  |not tested  |not tested  |
	| Scala 2.12 | not tested  | not tested |  not tested | ✅ | ✅ | ✅ |
