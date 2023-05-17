# Storing large raster geometries in Parquet files

!!!warning
    Always convert the raster geometries to a well known format with the RS_AsXXX functions before saving them.
    It is possible to save the raw bytes of the raster geometries, but they will be stored in an internal Sedona format that is not guaranteed to be stable across versions.

The default settings in Spark are not well suited for storing large binaries like raster geometries.
It is very much worth the time to tune and benchmark your settings.
Writing large binaries with the default settings will result in poorly structured Parquet files that are very expensive to read.
Some basic tuning can increase the read performance by several magnitudes.

## Background

Parquet files are divided into one or several row groups.
Each column in a row group is stored in a column chunk.
Each column chunk is further divided into pages.
A page is conceptually an indivisible unit in terms of compression and encoding.
The default size for a page is 1 MB.
Data is buffered until the page is full and then written to disk.
The frequency of checks of the page size limit will be between `parquet.page.size.row.check.min` and `parquet.page.size.row.check.max` (default between 100 and 10000 rows).

If you write 5 MB image files to Parquet with the default setting the first page size check will happen after 100 rows.
You will end up with pages of 500 MB instead of 1 MB.
Reading such a file will require a lot of memory and will be slow.

## Reading poorly structured Parquet files

Especially snappy compressed files are sensitive to oversized pages.
More performant options are no compression or zstd compression.
You can set `spark.buffer.size` to a value larger than the default of 64k to improve read performance.
Increasing `spark.buffer.size` might add an io penalty for other columns in the Parquet file.

## Writing better structured Parquet files for blobs

Ideally you want to write Parquet files with a sane page size to get better and more consistent read performance across different clients.
Since version 1.12.0 of parquet-hadoop, bundled with Spark 3.2, you can add Hadoop properties for controlling page size checks.
Better values for writing blobs are:

```
spark.sql.parquet.compression.codec=zstd
spark.hadoop.parquet.page.size.row.check.min=2
spark.hadoop.parquet.page.size.row.check.max=10
```

Zstd performs better than snappy in general.
Even more so for large pages.
The first page size check will happen after 2 rows.
If the page is not full after 2 rows the next check will happen after another 2-10 rows, depending on the size of the two rows already written.

Spark will set Hadoop properties from Spark properties prefixed with "spark.hadoop.".
For a full list of Parquet Hadoop properties see: https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md
