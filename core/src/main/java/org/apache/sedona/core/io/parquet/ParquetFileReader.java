package org.apache.sedona.core.io.parquet;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.sedona.core.constants.SedonaConstants;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class ParquetFileReader {
    /**
     * Reads Parquet File with given geometry Column and the relevant User columns
     * @param sc
     * @param geometryColumn
     * @param userColumns
     * @param inputPaths
     * @return Avro Record RDD which needs to be deserialized into a GeometryRDD
     * @throws IOException
     */
    public static JavaRDD<GenericRecord> readFile(JavaSparkContext sc,
                                                  String geometryColumn,
                                                  List<String> userColumns,
                                                  String... inputPaths) throws SedonaException {
        try {
            final Job job = Job.getInstance(sc.hadoopConfiguration());
            Path path = null;
            Optional<Path> firstFile = Optional.empty();
            for(String inputPath:inputPaths){
                path = new Path(inputPath);
                FileSystem fs = path.getFileSystem(sc.hadoopConfiguration());
                firstFile = Arrays.stream(fs.globStatus(path))
                      .filter(fileStatus ->
                                      job.getConfiguration()
                                         .getBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false)
                                      || fileStatus.isFile())
                .map(fileStatus -> {
                    try {
                        if(fileStatus.isFile()){
                            return fileStatus;
                        }
                        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listLocatedStatus(fileStatus.getPath());
                        return fileStatusRemoteIterator.hasNext()?fileStatusRemoteIterator.next():null;
                    } catch (IOException e) {
                        return null;
                    }
                }).filter(locatedFileStatus -> locatedFileStatus!=null).findFirst().map(fileStatus -> fileStatus.getPath());
                if(firstFile.isPresent()){
                    break;
                }
            }
            ParquetInputFormat.setInputPaths(job, String.join(SedonaConstants.COMMA, inputPaths));
            if(firstFile.isPresent()){
                Schema schema = Schemas.fromParquet(job.getConfiguration(), firstFile.get().toUri());
                List<String> columns = Lists.newArrayList(geometryColumn);
                columns.addAll(userColumns);
                schema = Expressions.filterSchema(schema, columns);
                AvroParquetInputFormat.setRequestedProjection(job, schema);
            }
        
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
        } catch (IOException e) {
            throw new SedonaException(e);
        }
    }
    
}
