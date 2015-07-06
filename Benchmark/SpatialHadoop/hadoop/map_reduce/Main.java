package hadoop.map_reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class Main {
	private static String hdfspath = "hdfs://master:9000/user/ubuntu/";

	private static Text word = new Text();
	public static double distance = 0.05;
	private static void deletehdfsFile(String path) throws IOException {
		Runtime rt = Runtime.getRuntime();
		Process pr = rt.exec("hadoop fs -rmr " + path);
	}
	public static class MapForPoly extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split(",");
			Double result = Double.parseDouble(token[1]);
			word.set(token[0]);
			output.collect(word, new Text(result.toString()));
		}
	}

	public static class ReduceForPoly extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Double mx = Double.parseDouble(key.toString());
			while (values.hasNext()) {
				Double my = Double.parseDouble(values.next().toString());
				Double x1 = mx - distance;
				Double y1 = my - distance;
				Double x2 = mx + distance;
				Double y2 = my + distance;
				String Polygon = "POLYGON((" + x1.toString() + " "
						+ y1.toString() + ", " + x1.toString() + " "
						+ y2.toString() + ", " + x2.toString() + " "
						+ y2.toString() + ", " + x2.toString() + " "
						+ y1.toString() + ", " + x1.toString() + " "
						+ y1.toString() + "))";

				output.collect(new Text(Polygon), new Text());
			}
		}
	}

	public static void main(String args[]) throws IOException {

		JobConf conf5 = new JobConf(Colocation.class);
		conf5.setJobName("phase5-0");
		conf5.setMapperClass(MapForPoly.class);
		conf5.setReducerClass(ReduceForPoly.class);

		conf5.setInputFormat(TextInputFormat.class);
		conf5.setOutputKeyClass(Text.class);
		conf5.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf5, new Path(hdfspath + "point1"));
		FileOutputFormat
				.setOutputPath(conf5, new Path(hdfspath + "point1poly"));
		deletehdfsFile(hdfspath + "point1poly");
		JobClient.runJob(conf5);
	}
}
