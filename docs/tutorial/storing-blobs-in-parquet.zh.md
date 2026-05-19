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

# 在 Parquet 文件中存储大型栅格几何对象

!!!warning
    在保存栅格几何对象之前，务必先用 `RS_AsXXX` 函数将其转换为公认的标准格式。
    虽然可以保存栅格几何对象的原始字节，但这是 Sedona 的内部格式，不保证跨版本稳定。

Spark 的默认设置并不适合存储栅格几何对象这类大型二进制数据。
为此投入时间调优和基准测试是非常值得的。
使用默认设置写入大型二进制数据会得到结构很差、读取代价非常高的 Parquet 文件。
做一些基本调优可以让读取性能提升数个数量级。

## 背景

Parquet 文件被划分为一个或多个 row group。
每个 row group 中的每一列存储为一个 column chunk，
每个 column chunk 又进一步划分为 page。
page 在压缩与编码层面是不可分割的最小单位，默认大小为 1 MB。
数据先在内存中缓冲，写满 page 后再落盘写入。
对 page 大小的检查频率介于 `parquet.page.size.row.check.min` 与 `parquet.page.size.row.check.max` 之间（默认在 100 到 10000 行之间）。

如果您按默认设置将 5 MB 的图像文件写入 Parquet，第一次 page 大小检查会在 100 行后才发生。
这样得到的 page 会是 500 MB 而非 1 MB。
读取这种文件会消耗大量内存，速度也会很慢。

## 读取结构不佳的 Parquet 文件

snappy 压缩对超大 page 尤为敏感。
更合适的选择是不压缩或使用 zstd 压缩。
您可以将 `spark.buffer.size` 设置为大于默认 64k 的值以提升读取性能。
不过调大 `spark.buffer.size` 可能会让 Parquet 文件中的其他列承担额外的 I/O 开销。

## 为大块二进制数据写出结构更佳的 Parquet 文件

理想情况下，您希望以合理的 page 大小写出 Parquet 文件，从而在不同客户端读取时都能获得更好且更一致的性能。
自 parquet-hadoop 1.12.0 起（Spark 3.2 内置该版本），可以通过 Hadoop 属性来控制 page 大小检查。
更适合写大块数据的设置如下：

```
spark.sql.parquet.compression.codec=zstd
spark.hadoop.parquet.page.size.row.check.min=2
spark.hadoop.parquet.page.size.row.check.max=10
```

整体上 zstd 比 snappy 表现更好，对于大型 page 更是如此。
第一次 page 大小检查会在 2 行后进行；如果 2 行后 page 仍未写满，下一次检查会在再写 2 到 10 行后发生（具体取决于已写入两行的字节数）。

Spark 会把以 “spark.hadoop.” 为前缀的 Spark 属性映射成 Hadoop 属性。
完整的 Parquet Hadoop 属性列表请参考：https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md
