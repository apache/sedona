package hadoop.map_reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.URI;

/**
 * Created by jinxuanw on 6/15/15.
 */
public class ExtractRectangle {
    private String input;
    private String output;
    private String hdfs;

    public ExtractRectangle(String hdfs, String input, String output){
        this.hdfs = hdfs;
        this.input = input;
        this.output = output;
    }
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, NullWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, NullWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] token = line.split(",");
            String x1 = token[1];
            String y1 = token[2];
            String x2 = token[3];
            String y2 = token[4];
            output.collect(new Text(x1+","+y1+","+x2+","+y2),
                    NullWritable.get());
        }
    }

    private static void deletehdfsFile(String path) throws IOException {
        URI uri = URI.create(path);
        Path pathhadoop = new Path(uri);
        Configuration confhadoop = new Configuration();
        FileSystem filehadoop = FileSystem.get(uri, confhadoop);
        filehadoop.delete(pathhadoop, true);
    }

    public void run() throws IOException {
        // Conver WTK rectangle to normal rectangle
        JobConf conf = new JobConf(WTKtoRectangle.class);
        conf.setJobName("WTK to normal rectangle");

        conf.setMapperClass(Map.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(conf, new Path(
                hdfs + input));
        FileOutputFormat.setOutputPath(conf, new Path(
                hdfs + output));
        deletehdfsFile(hdfs + output);
        JobClient.runJob(conf);
    }

    public static void main(String[] args) throws Exception {
    	/*
    	 args[0] is input path of hdfs, should begin with hdfs://
    	 args[1] is the name of input file
    	 args[2] is the name of output file. 
    	 */
    	System.out.println("This is a preprocess for corelation application, input is RectangleNum, x1, y1, x2, y2");
    	System.out.println("Output is x1, y2, x2, y2");
    	System.out.println("Three input arguments are needed");
    	System.out.println("args[0] is input path of hdfs, should begin with hdfs://");
    	System.out.println("args[1] is the name of input file");
    	System.out.println("args[2] is the name of output file.");
    	System.out.println("Input format should be like 1,x1,y1,x2,y2");
       ExtractRectangle extractRectangle = new ExtractRectangle(args[0], args[1], args[2]);
        extractRectangle.run();
    }
}
