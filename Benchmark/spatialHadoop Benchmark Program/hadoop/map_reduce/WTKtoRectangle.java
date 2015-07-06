package hadoop.map_reduce;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class WTKtoRectangle {
	private String input;
	private String output;
	private String hdfs;
	
	public WTKtoRectangle(String hdfs, String input, String output){
		this.hdfs = hdfs;
		this.input = input;
		this.output = output;
	}
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, NullWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split("\t");
			String wtk = token[0];
			String rectangle = wtk.substring(10, wtk.length() - 2);
			String x1y1 = rectangle.split(",")[0].trim();
			String x2y2 = rectangle.split(",")[2].trim();
			String x1 = x1y1.split(" ")[0].trim();
			String y1 = x1y1.split(" ")[1].trim();
			String x2 = x2y2.split(" ")[0].trim();
			String y2 = x2y2.split(" ")[1].trim();
//			Double dx1 = Double.parseDouble(x1);
//			Double dx2 = Double.parseDouble(x2);
//			Double dy1 = Double.parseDouble(y1);
//			Double dy2 = Double.parseDouble(y2);
//			Integer index = Integer.parseInt(token[1]);
//			Text text = new Text();
//			
//			TextSerializerHelper.serializeDouble(dx1, text, ',');
//		    TextSerializerHelper.serializeDouble(dy1, text, ',');
//		    TextSerializerHelper.serializeDouble(dx2, text, ',');
//		    TextSerializerHelper.serializeDouble(dy2, text, ',');
//		    TextSerializerHelper.serializeInt(index, text, '\0');
		    //new Text(x1+","+y1+","+x2+","+y2+"\t"+token[1])
			output.collect(new Text(x1+","+y1+","+x2+","+y2+"\t"+token[1]),
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
		conf.setJobName("Parse Aggregation Result");

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
		WTKtoRectangle wtk= new WTKtoRectangle(args[0], args[1], args[2]);
		wtk.run();
	}
}
