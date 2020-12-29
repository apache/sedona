## Benchmark

We welcome people to use Sedona for benchmark purpose. To achieve the best performance or enjoy all features of Sedona,

* Please always use the latest version or state the version used in your benchmark so that we can trace back to the issues.
* Please consider using Sedona core instead of Sedona SQL. Due to the limitation of SparkSQL (for instance, not support clustered index), we are not able to expose all features to SparkSQL.
* Please open Sedona kryo serializer to reduce the memory footprint.