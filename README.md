### Spatial RDD
Custom Spark RDD to org.apache.sedona.core.partition geospatial data based on org.apache.sedona.core.spatial proximity for faster orthogonal range queries.

### What is this Fork about?
This fork modifies the Spatial RDD to org.apache.sedona.core.partition dataset using `KD Tree` & `Epsilon approximation` based on [Parallel Algorithms for Constructing Range and
Nearest-Neighbor Searching Data Structures](https://users.cs.duke.edu/~pankaj/publications/papers/mr-ds.pdf).
