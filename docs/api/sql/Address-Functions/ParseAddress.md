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

# ParseAddress

Introduction: Returns an array of the components (e.g. street, postal code) of the input address string. This is backed by the [libpostal](https://github.com/openvenues/libpostal) library's address parsing functionality.

!!!Note
    Jpostal requires at least 2 GB of free disk space to store the data files used for address parsing and expanding. By default, the data files are downloaded automatically to a temporary directory (`<java.io.tmpdir>/libpostal/`, e.g. `/tmp/libpostal/` on Linux/macOS) when the library is initialized.

!!!Note
    The version of jpostal installed with this package only supports Linux and macOS. If you are using Windows, you will need to install libjpostal and libpostal manually and ensure that they are available in your `java.library.path`.

The data directory can be configured via `spark.sedona.libpostal.dataDir`. You can point it to a remote filesystem path (HDFS, S3, GCS, ABFS, etc.) such as `hdfs:///data/libpostal/` or `s3a://my-bucket/libpostal/`. When using a remote path, you must distribute the data to all executors before running queries by calling `sc.addFile("hdfs:///data/libpostal/", recursive=True)` (PySpark) or `sc.addFile("hdfs:///data/libpostal/", recursive = true)` (Scala). In this remote-URI mode, the automatic internet download performed by jpostal is disabled, so the remote directory must already contain the libpostal model files. For local filesystem paths, jpostal's download-if-needed behavior remains enabled.

To prepare the libpostal data for a remote filesystem, first download it to a local machine by following the [libpostal installation instructions](https://github.com/openvenues/libpostal#installation-maclinux). After installation, the data files will be in the directory you specified during setup (commonly `/tmp/libpostal/`). Then upload them to your remote storage.

Format: `ParseAddress (address: String)`

Return type: `Array<Struct<label: String, value: String>>`

Since: `v1.8.0`

SQL Example

```sql
SELECT ParseAddress("100 W 1st St, Los Angeles, CA 90012");
```

Output:

```
[{house_number, 100}, {road, w 1st st}, {city, los angeles}, {state, ca}, {postcode, 90012}]
```
