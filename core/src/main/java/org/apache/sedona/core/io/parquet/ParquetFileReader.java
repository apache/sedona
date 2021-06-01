package org.apache.sedona.core.io.parquet;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.sedona.core.constants.SedonaConstants;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ParquetFileReader {
    public static JavaRDD<GenericRecord> readFile(JavaSparkContext sc,
                                                  String geometryColumn,
                                                  List<String> userColumns,
                                                  String... inputPaths) throws IOException {
        final Job job = Job.getInstance(sc.hadoopConfiguration());
        ParquetInputFormat.setInputPaths(job, String.join(SedonaConstants.COMMA, inputPaths));
        Schema schema = Schemas.fromParquet(job.getConfiguration(), new Path(inputPaths[0]).toUri());
        List<String> columns = Lists.newArrayList(geometryColumn);
        columns.addAll(userColumns);
        schema = Expressions.filterSchema(schema, columns);
        AvroParquetInputFormat.setRequestedProjection(job, schema);
        return sc.newAPIHadoopRDD(job.getConfiguration(), AvroParquetInputFormat.class, LongWritable.class,
                                  GenericRecord.class)
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<LongWritable, GenericRecord>>, GenericRecord>() {
                    @Override
                    public Iterator<GenericRecord> call(Iterator<Tuple2<LongWritable, GenericRecord>> recordIterator) throws Exception {
                        Iterable<Tuple2<LongWritable, GenericRecord>> recordIterable = () -> recordIterator;
                        return StreamSupport.stream(recordIterable.spliterator(), false)
                                .map(recordTuple -> recordTuple._2)
                                .iterator();
                    }
                });
    }
    
}
