package hadoop.map_reduce;

import java.io.*;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Aggregation {
	private final static LongWritable one = new LongWritable(1);
	private static Text word = new Text();
	private String hdfspath;
	private String inputRec1;
	private String inputRec2;
	private String outputRec;

	Aggregation(String hdfs, String inputRec1, String inputRec2,
				String outputRec) {
		this.hdfspath = hdfs;
		this.inputRec1 = inputRec1;
		this.inputRec2 = inputRec2;
		this.outputRec = outputRec;
	}

	public String gethdfs() {
		return this.hdfspath;
	}

	public static class MapForCount extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split("	");
			output.collect(new Text(token[0]), one);
		}
	}

	// Reduce Class to Count the total Number of Records.
	public static class ReduceForCount extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += Long.valueOf(values.next().get());
			}
			output.collect(key, new LongWritable(sum));
		}
	}

	private static void deletehdfsFile(String path) throws IOException {
		URI uri = URI.create(path);
		Path pathhadoop = new Path(uri);
		Configuration confhadoop = new Configuration();
		FileSystem filehadoop = FileSystem.get(uri, confhadoop);
		filehadoop.delete(pathhadoop, true);
	}

	public static class MapZctaRectangle extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, NullWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split(",");
			String x1 = token[2];
			String y1 = token[3];
			String x2 = token[4];
			String y2 = token[5];

			String result = "POLYGON((" + x1 + " " + y1 + "," + x1 + " " + y2
					+ "," + x2 + " " + y2 + "," + x2 + " " + y1 + "," + x1
					+ " " + y1 + "))";
			//String result = x1+","+y1+","+x2+","+y2;
			output.collect(new Text(result), NullWritable.get());
		}
	}

	public static class MapAreaWater extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, NullWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split(",");
			String x1 = token[2];
			String y1 = token[3];
			String x2 = token[4];
			String y2 = token[5];
			String result = "POINT(" + token[2] + " " + token[3] + ")";
			//String result = x1+","+y1+","+x2+","+y2;
			output.collect(new Text(result), NullWritable.get());
		}
	}

	public static class MapAllNodes extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split("\t");
			String result = "POINT(" + token[1] + " " + token[2] + ")";
			output.collect(new Text(result), new Text());
		}
	}

	public void run() throws IOException {
		long projectStartTime = System.currentTimeMillis();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("[Measure]" + "Project begin at "
				+ dateFormat.format(date));
		PrintWriter timeWriter = null;
		
		try {
			timeWriter= new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			timeWriter.println(dateFormat.format(date));
			timeWriter.close();
		} catch (IOException e) {
			//exception handling left as an exercise for the reader
		} 
		
		//begin phase 1
		JobConf conf0 = new JobConf(Aggregation.class);
		conf0.setJobName("Aggregation Phase1-1:Parse Input");

		conf0.setMapperClass(MapZctaRectangle.class);
		conf0.setNumReduceTasks(0);
//		conf0.set("fs.default.name", "hdfs://master:9000");
//		conf0.set("mapred.job.tracker", "master:9001");
		conf0.setInputFormat(TextInputFormat.class);
		conf0.setOutputKeyClass(Text.class);
		conf0.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(conf0, new Path(hdfspath + inputRec1));
		deletehdfsFile(hdfspath + inputRec1 + ".parse");
		FileOutputFormat.setOutputPath(conf0, new Path(hdfspath + inputRec1
				+ ".parse"));
		JobClient.runJob(conf0);
		
		//Begin to parse dataset areawater.
		JobConf conf = new JobConf(Aggregation.class);
		conf.setJobName("Aggregation Phase1-2:Parse Input");
//
//		conf.set("fs.default.name", "hdfs://master:9000");
//		conf.set("mapred.job.tracker", "master:9001");
		conf.setMapperClass(MapAreaWater.class);
		conf.setNumReduceTasks(0);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(conf, new Path(hdfspath + inputRec2));
		FileOutputFormat.setOutputPath(conf, new Path(hdfspath + inputRec2
				+ ".parse"));
		deletehdfsFile(hdfspath + inputRec2 + ".parse");
		JobClient.runJob(conf);
		long phase1EndTime = System.currentTimeMillis();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		timeWriter.println(dateFormat.format(date));
		timeWriter.flush();
		System.out.println("[Measure]" + "Phase 1 finish at "
				+ dateFormat.format(date));

		try {
			timeWriter= new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			timeWriter.println(dateFormat.format(date));
			timeWriter.close();
		} catch (IOException e) {
			//exception handling left as an exercise for the reader
		} 
		//Begin phase 2, do a join operation on the parsed data set.
		try {
			Process p = Runtime.getRuntime().exec("shadoop sjmr "+ inputRec1 + ".parse" + " " + inputRec2 + ".parse" + " " + "aggtemp" + " shape:wkt " + "-overwrite");
			InputStream stderr = p.getErrorStream();
			InputStream stdout = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(stderr);
            InputStreamReader iso = new InputStreamReader(stdout);
            BufferedReader br = new BufferedReader(isr);
            BufferedReader br2 = new BufferedReader(iso);
            String line1, line2 = null;
            while ( (line1 = br.readLine()) != null || (line2 = br2.readLine()) != null){
            	if(line1 != null)
            		System.err.println(line1);
            	if(line2 != null)
            		System.err.println(line1);
            }
			p.waitFor();
		} catch (Exception e) {
		}
		
		
		long phase2EndTime = System.currentTimeMillis();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 2 finish at "
				+ dateFormat.format(date));

		try {
			timeWriter= new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			timeWriter.println(dateFormat.format(date));
			timeWriter.close();
		} catch (IOException e) {
			//exception handling left as an exercise for the reader
		} 
		// Phase 3: aggregate result from phase 3.
		JobConf conf1 = new JobConf(Aggregation.class);
		conf1.setJobName("Aggregation Phase3:Parse Input");
		conf1.setMapperClass(MapForCount.class);
		conf1.setCombinerClass(ReduceForCount.class);
		conf1.setReducerClass(ReduceForCount.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(LongWritable.class);
//		conf1.set("fs.default.name", "hdfs://master:9000");
//		conf1.set("mapred.job.tracker", "master:9001");
		FileInputFormat.setInputPaths(conf1, new Path(hdfspath + "aggtemp"));
		FileOutputFormat.setOutputPath(conf1, new Path(hdfspath + outputRec));
		deletehdfsFile(hdfspath + outputRec);
		JobClient.runJob(conf1);
		
		
		long phase3EndTime = System.currentTimeMillis();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 3 finish at "
				+ dateFormat.format(date));
		try {
			timeWriter= new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			timeWriter.println(dateFormat.format(date));
			timeWriter.close();
		} catch (IOException e) {
			//exception handling left as an exercise for the reader
		} 
		long projectTime = (phase3EndTime - projectStartTime)/1000;
		long phase1Time = (phase1EndTime - projectStartTime)/1000;
		long phase2Time = (phase2EndTime - phase1EndTime)/1000;
		long phase3Time = (phase3EndTime - phase2EndTime)/1000;
		
		PrintWriter executeTimeWriter = null;
		
		try {
			executeTimeWriter= new PrintWriter(new BufferedWriter(new FileWriter("ExecuteTime.txt", true)));
			executeTimeWriter.println(phase1Time + "\t" + phase2Time + "\t" + phase3Time);
			executeTimeWriter.close();
		} catch (IOException e) {
			e.printStackTrace();//exception handling left as an exercise for the reader
		}

		PrintWriter totalTimeWriter = null;
		try{
			totalTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("TotalTime.txt", true)));
			totalTimeWriter.println(projectTime);
			totalTimeWriter.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		
		
		System.out.println("[Measure]" + "Phase 1 running time is: "
				+ phase1Time + "s");
		System.out.println("[Measure]" + "Phase 2 running time is: "
				+ phase2Time + "s");
		System.out.println("[Measure]" + "Phase 3 running time is: "
				+ phase3Time + "s");
		System.out.println("[Measure]" + "Total project running time is: "
				+ projectTime + "s");

	}
	
	public void usage(){
		
	}
	public static void main(String args[]) throws IOException {
    	/*
   	 args[0] is input path of hdfs, should begin with hdfs://
   	 args[1] is the name of input file
   	 args[2] is the name of output file. 
   	 */
	   	System.out.println("This is aggregation application");
	   	System.out.println("Output is x1, y2, x2, y2");
	   	System.out.println("Four input arguments are needed");
	   	System.out.println("args[0] is input path of hdfs, should begin with hdfs://");
	   	System.out.println("args[1] is the name of input file 1");
	   	System.out.println("args[2] is the name of input file 2");
	   	System.out.println("args[3] is the name of output file.");
	   	System.out.println("The input file could have many fields, but it should be in format: field1, field2, x1, y1, x2, y2, *; ");
		Aggregation agg = new Aggregation(args[0], args[1], args[2], args[3]);
		agg.run();
	}
}
