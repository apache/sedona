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

Sedona binary releases are compiled by Java 11/17 and Scala 2.12/2.13 and tested in the following environments:

**Java Requirements:**

- Spark 3.4 & 3.5: Java 11
- Spark 4.0: Java 17

**Note:** Java 8 support is dropped since Sedona 1.8.0. Spark 3.3 support is dropped since Sedona 1.8.0.

=== "Sedona Scala/Java"

	|             | Spark 3.4| Spark 3.5 | Spark 4.0 |
	|:---------:|:---------:|:---------:|:---------:|
	| Scala 2.12 |✅ |✅ |✅ |
	| Scala 2.13 |✅ |✅ |✅ |

=== "Sedona Python"

    |             | Spark 3.4 (Scala 2.12)|Spark 3.5 (Scala 2.12)| Spark 4.0 (Scala 2.12)|
    |:---------:|:---------:|:---------:|:---------:|
    | Python 3.7  |  ✅  |  ✅  | ✅ |
    | Python 3.8 |  ✅  |  ✅  | ✅  |
    | Python 3.9 |  ✅  |  ✅  | ✅  |
    | Python 3.10 |  ✅  |  ✅  | ✅  |

=== "Sedona R"

	|             | Spark 3.4 | Spark 3.5 | Spark 4.0 |
	|:---------:|:---------:|:---------:|:---------:|
	| Scala 2.12 | ✅ | ✅ | ✅ |
