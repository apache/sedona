package hadoop.map_reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

public class Colocation {
	private final static LongWritable one = new LongWritable(1);
	private static Text word = new Text();
	private double n1 = 0;
	private double n2 = 0;
	public  String distance;
	private String inputRec1;
	private String inputRec2;
	private static String namenode = "hdfs://master:9000";
	private String hdfspath = "hdfs://192.168.56.101:9000/user/jinxuanw/";

	Colocation(String hdfspath, String inputRec1, String inputRec2, String Distance) {
		this.hdfspath = hdfspath;
		this.inputRec1 = inputRec1;
		this.inputRec2 = inputRec2;
		distance = Distance;
	}



	void setnamenode(String name) {
		namenode = name;
	}


	public static class MapForCount extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			word.set("total");
			output.collect(word, one);

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
		Runtime rt = Runtime.getRuntime();
		Process pr = rt.exec("hadoop fs -rmr " + path);
		InputStream stderr = pr.getErrorStream();
		InputStream stdout = pr.getInputStream();
        InputStreamReader isr = new InputStreamReader(stderr);
        InputStreamReader iso = new InputStreamReader(stdout);
        BufferedReader br = new BufferedReader(isr);
        String line1, line2 = null;
        while ( (line1 = br.readLine()) != null || (line2 = br.readLine()) != null){
        	
        }
        try {
			pr.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static ArrayList<String> readFile(String file) throws IOException {
		ArrayList<String> lines = new ArrayList<String>();
		try {
			Path pt = new Path(file);
			FileSystem fs = FileSystem.get(new URI(namenode),
					new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();
			while (line != null) {
				lines.add(line);
				line = br.readLine();
			}

		} catch (Exception e) {
		}
		return lines;
	}

	public static class MapForMax extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split(",");
			double result = Double.parseDouble(token[0]);
			word.set("total");
			output.collect(word, new DoubleWritable(result));
		}
	}

	public static class MapForMax2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split(",");
			double result = Double.parseDouble(token[1]);
			word.set("total");
			output.collect(word, new DoubleWritable(result));
		}
	}

	// Reduce Class to Count the total Number of Records.
	public static class ReduceForMax extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			BigDecimal curmax = BigDecimal.valueOf(Double.MIN_VALUE);
			BigDecimal zero = BigDecimal.valueOf(0);
			while (values.hasNext()) {
				double tmp = values.next().get();
				BigDecimal next = BigDecimal.valueOf(tmp);
				if (curmax.subtract(next).compareTo(zero) < 0) {
					curmax = next;
				}
			}
			double dcurmax = curmax.doubleValue();
			output.collect(key, new DoubleWritable(dcurmax));
		}
	}

	public static class MapForMin extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split(",");
			double result = Double.parseDouble(token[0]);
			word.set("total");
			output.collect(word, new DoubleWritable(result));
		}
	}

	public static class MapForMin2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split(",");
			double result = Double.parseDouble(token[1]);
			word.set("total");
			output.collect(word, new DoubleWritable(result));
		}
	}

	public static class ReduceForMin extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			BigDecimal curmin = BigDecimal.valueOf(Double.MAX_VALUE);
			BigDecimal zero = BigDecimal.valueOf(0);
			while (values.hasNext()) {
				double tmp = values.next().get();
				BigDecimal next = BigDecimal.valueOf(tmp);
				if (next.subtract(curmin).compareTo(zero) < 0) {
					curmin = next;
				}
			}
			double dcurmin = curmin.doubleValue();
			output.collect(key, new DoubleWritable(dcurmin));
		}
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
		private static double d;
		public void configure(JobConf job) {
			d = Double.parseDouble(job.get("distance"));
		}
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Double mx = Double.parseDouble(key.toString());
			
			while (values.hasNext()) {
				Double my = Double.parseDouble(values.next().toString());
				//System.out.println("distance is " + Colocation.distance);
				Double x1 = mx - d;
				Double y1 = my - d;
				Double x2 = mx +  d;
				Double y2 = my + d ;
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

	public static class MapForPoint extends MapReduceBase implements
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

	public static class ReduceForPoint extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Double mx = Double.parseDouble(key.toString());
			while (values.hasNext()) {
				Double my = Double.parseDouble(values.next().toString());
				String Point = "POINT(" + mx.toString() + " " + my.toString()
						+ ")";
				output.collect(new Text(Point), new Text());
			}
		}
	}

	public static class MapForFilter extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		private static double d;
		public void configure(JobConf job) {
			d = Double.parseDouble(job.get("distance"));
		}
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split("	");
			String mxs1 = token[0].split(", ")[1].split(" ")[0];
			String mxs2 = token[0].split(", ")[1].split(" ")[1];

			Double mx = Double.parseDouble(mxs1);
			Double my = Double.parseDouble(mxs2);
			
			Double x = mx + d;
			Double y = my - d;

			String pointstr[] = token[1].substring(7, token[1].length() - 1)
					.split(" ");
			Double x1 = Double.parseDouble(pointstr[0]);
			Double y1 = Double.parseDouble(pointstr[1]);

			Double xx = (x1 - x) * (x1 - x);
			Double yy = (y1 - y) * (y1 - y);
			Double realdistance = Math.sqrt(xx + yy);
			word.set("total");
			if (realdistance < d)
				output.collect(word, one);
		}
	}

	public static class ReduceForFilter extends MapReduceBase implements
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

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split("\t");
			String result = token[1] + "," + token[2];
			Random r = new Random();
			Integer i = r.nextInt(16);
			output.collect(new Text(i.toString()), new Text(result));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(null, new Text(values.next()));
			}
		}
	}

	public static class Map1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, NullWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] token = line.split(",");
			String result = token[2] + "," + token[3];
			output.collect( new Text(result), NullWritable.get());
		}
	}

	public static class Reduce1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(null, new Text(values.next()));
			}
		}
	}

	public void run() throws IOException {
		PrintWriter phaseTimeWriter = null;

		long ProjectStartTime = System.currentTimeMillis();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		//Begin phase 0, parse the input and extract spatial point part
		System.out.println("[Measure]" + "Project start at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		JobConf conf0 = new JobConf(Colocation.class);
		conf0.setJobName("phase0-0");
		conf0.setMapperClass(Map1.class);
		//conf0.setReducerClass(Reduce1.class);

		conf0.setInputFormat(TextInputFormat.class);
		conf0.setOutputKeyClass(Text.class);
		conf0.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(conf0, new Path(hdfspath + inputRec1));
		FileOutputFormat.setOutputPath(conf0, new Path(hdfspath + "point1"));
		deletehdfsFile(hdfspath + "point1");
		JobClient.runJob(conf0);

		JobConf conf00 = new JobConf(Colocation.class);
		conf00.setJobName("phase0-1");
		conf00.setMapperClass(Map1.class);
		//conf00.setReducerClass(Reduce1.class);

		conf00.setInputFormat(TextInputFormat.class);
		conf00.setOutputKeyClass(Text.class);
		conf00.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(conf00, new Path(hdfspath + inputRec2));
		FileOutputFormat.setOutputPath(conf00, new Path(hdfspath + "point2"));
		deletehdfsFile(hdfspath + "point2");
		JobClient.runJob(conf00);
		long Phase0EndTime = System.currentTimeMillis();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 0 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		//Begin phase 1
		JobConf conf1 = new JobConf(Colocation.class);
		conf1.setJobName("phase1-0");
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);

		conf1.setMapperClass(MapForCount.class);
		conf1.setCombinerClass(ReduceForCount.class);
		conf1.setReducerClass(ReduceForCount.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(conf1, new Path(hdfspath + "point1"));
		FileOutputFormat.setOutputPath(conf1, new Path(hdfspath
				+ ".countpoint1"));
		deletehdfsFile(hdfspath + ".countpoint1");
		JobClient.runJob(conf1);

		long Phase1EndTime = System.currentTimeMillis();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 1 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Second MapReduce Parse count the total number of records in point set
		// 2;
		JobConf conf2 = new JobConf(Colocation.class);
		conf2.setJobName("phase2-0");
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);

		conf2.setMapperClass(MapForCount.class);
		conf2.setCombinerClass(ReduceForCount.class);
		conf2.setReducerClass(ReduceForCount.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(conf2, new Path(hdfspath + "point2"));
		FileOutputFormat.setOutputPath(conf2, new Path(hdfspath
				+ ".countpoint2"));
		deletehdfsFile(hdfspath + ".countpoint2");
		JobClient.runJob(conf2);

		long Phase2EndTime = System.currentTimeMillis();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 2 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Calculate n1 and n2
		ArrayList<String> lines = readFile(hdfspath + ".countpoint1/part-00000");
		String[] totalNum = lines.get(0).split("\t");
		n1 = Double.parseDouble(totalNum[1]);

		lines = readFile(hdfspath + ".countpoint2/part-00000");
		totalNum = lines.get(0).split("\t");
		n2 = Double.parseDouble(totalNum[1]);

		//System.out.println("n1 is " + n1);
		//System.out.println("n2 is " + n2);

		// Step 3, Calculate the min and max value in point set 1 and point set
		// Max value in point set 1 first column:
		JobConf conf3 = new JobConf(Colocation.class);
		conf3.setJobName("phase3-0");
		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(IntWritable.class);

		conf3.setMapperClass(MapForMax.class);
		conf3.setCombinerClass(ReduceForMax.class);
		conf3.setReducerClass(ReduceForMax.class);

		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(conf3, new Path(hdfspath + "point1"));
		FileOutputFormat.setOutputPath(conf3, new Path(hdfspath + ".max_X1"));
		deletehdfsFile(hdfspath + ".max_X1");
		JobClient.runJob(conf3);
		
		JobConf conf33 = new JobConf(Colocation.class);
		conf33.setJobName("phase3-1");
		conf33.setOutputKeyClass(Text.class);
		conf33.setOutputValueClass(IntWritable.class);

		conf33.setMapperClass(MapForMax.class);
		conf33.setCombinerClass(ReduceForMax.class);
		conf33.setReducerClass(ReduceForMax.class);

		conf33.setInputFormat(TextInputFormat.class);
		conf33.setOutputKeyClass(Text.class);
		conf33.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(conf33, new Path(hdfspath + "point2"));
		FileOutputFormat.setOutputPath(conf33, new Path(hdfspath + ".max_X2"));
		deletehdfsFile(hdfspath + ".max_X2");
		JobClient.runJob(conf33);

		// Still in Conf, change the mapper class and calculate Max Y
//		conf3.setMapperClass(MapForMax2.class);
		JobConf conf34 = new JobConf(Colocation.class);
		conf34.setJobName("phase3-2");
		conf34.setOutputKeyClass(Text.class);
		conf34.setOutputValueClass(IntWritable.class);

		conf34.setMapperClass(MapForMax2.class);
		conf34.setCombinerClass(ReduceForMax.class);
		conf34.setReducerClass(ReduceForMax.class);

		conf34.setInputFormat(TextInputFormat.class);
		conf34.setOutputKeyClass(Text.class);
		conf34.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(conf34, new Path(hdfspath + "point1"));
		FileOutputFormat.setOutputPath(conf34, new Path(hdfspath + ".max_Y1"));
		deletehdfsFile(hdfspath + ".max_Y1");
		JobClient.runJob(conf34);
		
		
		JobConf conf35 = new JobConf(Colocation.class);
		conf35.setJobName("phase3-3");
		conf35.setOutputKeyClass(Text.class);
		conf35.setOutputValueClass(IntWritable.class);

		conf35.setMapperClass(MapForMax2.class);
		conf35.setCombinerClass(ReduceForMax.class);
		conf35.setReducerClass(ReduceForMax.class);

		conf35.setInputFormat(TextInputFormat.class);
		conf35.setOutputKeyClass(Text.class);
		conf35.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(conf35, new Path(hdfspath + "point2"));
		FileOutputFormat.setOutputPath(conf35, new Path(hdfspath + ".max_Y2"));
		deletehdfsFile(hdfspath + ".max_Y2");
		JobClient.runJob(conf35);

		long Phase3EndTime = System.currentTimeMillis();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 3 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		JobConf conf4 = new JobConf(Colocation.class);
		conf4.setJobName("phase4-0");
		conf4.setMapperClass(MapForMin.class);
		conf4.setCombinerClass(ReduceForMin.class);
		conf4.setReducerClass(ReduceForMin.class);

		conf4.setInputFormat(TextInputFormat.class);
		conf4.setOutputKeyClass(Text.class);
		conf4.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(conf4, new Path(hdfspath + "point1"));
		FileOutputFormat.setOutputPath(conf4, new Path(hdfspath + ".min_X1"));
		deletehdfsFile(hdfspath + ".min_X1");
		JobClient.runJob(conf4);
		
		JobConf conf41 = new JobConf(Colocation.class);
		conf41.setJobName("phase4-1");
		conf41.setMapperClass(MapForMin.class);
		conf41.setCombinerClass(ReduceForMin.class);
		conf41.setReducerClass(ReduceForMin.class);

		conf41.setInputFormat(TextInputFormat.class);
		conf41.setOutputKeyClass(Text.class);
		conf41.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(conf41, new Path(hdfspath + "point2"));
		FileOutputFormat.setOutputPath(conf41, new Path(hdfspath + ".min_X2"));
		deletehdfsFile(hdfspath + ".min_X2");
		JobClient.runJob(conf41);
		
		JobConf conf42 = new JobConf(Colocation.class);
		conf42.setJobName("phase4-2");
		conf42.setMapperClass(MapForMin.class);
		conf42.setCombinerClass(ReduceForMin.class);
		conf42.setReducerClass(ReduceForMin.class);

		conf42.setInputFormat(TextInputFormat.class);
		conf42.setOutputKeyClass(Text.class);
		conf42.setOutputValueClass(DoubleWritable.class);
		conf42.setMapperClass(MapForMin2.class);

		FileInputFormat.setInputPaths(conf42, new Path(hdfspath + "point1"));
		FileOutputFormat.setOutputPath(conf42, new Path(hdfspath + ".min_Y1"));
		deletehdfsFile(hdfspath + ".min_Y1");
		JobClient.runJob(conf42);
		
		JobConf conf43 = new JobConf(Colocation.class);
		conf43.setJobName("phase4-3");
		conf43.setMapperClass(MapForMin.class);
		conf43.setCombinerClass(ReduceForMin.class);
		conf43.setReducerClass(ReduceForMin.class);

		conf43.setInputFormat(TextInputFormat.class);
		conf43.setOutputKeyClass(Text.class);
		conf43.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(conf43, new Path(hdfspath + "point2"));
		FileOutputFormat.setOutputPath(conf43, new Path(hdfspath + ".min_Y2"));
		deletehdfsFile(hdfspath + ".min_Y2");
		JobClient.runJob(conf43);

		long Phase4EndTime = System.currentTimeMillis();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 4 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Read x1, x2, y1, y2's min and max value
		// max_x1
		lines = readFile(hdfspath + ".max_X1/part-00000");
		totalNum = lines.get(0).split("\t");
		double max_x1 = Double.parseDouble(totalNum[1]);
		// max_x2
		lines = readFile(hdfspath + ".max_X2/part-00000");
		totalNum = lines.get(0).split("\t");
		double max_x2 = Double.parseDouble(totalNum[1]);
		// max_y1
		lines = readFile(hdfspath + ".max_Y1/part-00000");
		totalNum = lines.get(0).split("\t");
		double max_y1 = Double.parseDouble(totalNum[1]);
		// max_y2
		lines = readFile(hdfspath + ".max_Y2/part-00000");
		totalNum = lines.get(0).split("\t");
		double max_y2 = Double.parseDouble(totalNum[1]);
		// min_x1
		lines = readFile(hdfspath + ".min_X1/part-00000");
		totalNum = lines.get(0).split("\t");
		double min_x1 = Double.parseDouble(totalNum[1]);
		// min_x2
		lines = readFile(hdfspath + ".min_X2/part-00000");
		totalNum = lines.get(0).split("\t");
		double min_x2 = Double.parseDouble(totalNum[1]);
		// min_y1
		lines = readFile(hdfspath + ".min_Y1/part-00000");
		totalNum = lines.get(0).split("\t");
		double min_y1 = Double.parseDouble(totalNum[1]);
		// min_y2
		lines = readFile(hdfspath + ".min_Y2/part-00000");
		totalNum = lines.get(0).split("\t");
		double min_y2 = Double.parseDouble(totalNum[1]);
		// Calculate the square

		double x1d = min_x1 < min_x2 ? min_x1 : min_x2;
		double x2d = max_x1 > max_x2 ? max_x1 : max_x2;
		double y1d = min_y1 < min_y2 ? min_y1 : min_y2;
		double y2d = max_y1 > max_y2 ? max_y1 : max_y2;

		BigDecimal x1 = BigDecimal.valueOf(x1d);
		BigDecimal x2 = BigDecimal.valueOf(x2d);
		BigDecimal y1 = BigDecimal.valueOf(y1d);
		BigDecimal y2 = BigDecimal.valueOf(y2d);
		BigDecimal X = x2.subtract(x1);
		BigDecimal Y = y2.subtract(y1);
		BigDecimal A = X.multiply(Y);
		double Ad = A.doubleValue();

		//System.out.println("A is " + Ad);

		// Calculate A_ij

		// Based on point set 1, create a rectangle set.

		JobConf conf5 = new JobConf(Colocation.class);
		conf5.setJobName("phase5-0");
		conf5.setMapperClass(MapForPoly.class);
		conf5.setReducerClass(ReduceForPoly.class);
		conf5.set("distance", this.distance);
		conf5.setInputFormat(TextInputFormat.class);
		conf5.setOutputKeyClass(Text.class);
		conf5.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf5, new Path(hdfspath + "point1"));
		FileOutputFormat
				.setOutputPath(conf5, new Path(hdfspath + "point1poly"));
		deletehdfsFile(hdfspath + "point1poly");
		JobClient.runJob(conf5);

		long Phase5EndTime = System.currentTimeMillis();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 5 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Based on point set 2, create a Point set.
		JobConf conf6 = new JobConf(Colocation.class);
		conf6.setJobName("phase6");
		conf6.setMapperClass(MapForPoint.class);
		conf6.setReducerClass(ReduceForPoint.class);

		conf6.setInputFormat(TextInputFormat.class);
		conf6.setOutputKeyClass(Text.class);
		conf6.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf6, new Path(hdfspath + "point2"));
		FileOutputFormat.setOutputPath(conf6,
				new Path(hdfspath + "point2point"));
		deletehdfsFile(hdfspath + "point2point");
		JobClient.runJob(conf6);

		long Phase6EndTime = System.currentTimeMillis();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 6 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		try {
			Process p = Runtime.getRuntime().exec("shadoop sjmr " + "point1poly " +  "point2point " + "polyjoinpoint "
					+ "shape:wkt" + " -overwrite");
			InputStream stderr = p.getErrorStream();
			InputStream stdout = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(stderr);
            InputStreamReader iso = new InputStreamReader(stdout);
            BufferedReader br1 = new BufferedReader(isr);
            BufferedReader br2 = new BufferedReader(iso);
            String line1, line2 = null;
            while ( (line1 = br1.readLine()) != null || (line2 = br2.readLine()) != null){
            }
			p.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long Phase7EndTime = System.currentTimeMillis();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 7 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Filter
		// Based on point set 2, create a Point set.
		JobConf conf8 = new JobConf(Colocation.class);
		conf8.setJobName("phase8");
		conf8.setMapperClass(MapForFilter.class);
		conf8.setReducerClass(ReduceForFilter.class);
		conf8.set("distance", this.distance);
		conf8.setInputFormat(TextInputFormat.class);
		conf8.setOutputKeyClass(Text.class);
		conf8.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(conf8, new Path(hdfspath
				+ "polyjoinpoint"));
		FileOutputFormat.setOutputPath(conf8, new Path(hdfspath
				+ ".polyjoinpoint"));
		deletehdfsFile(hdfspath + ".polyjoinpoint");
		JobClient.runJob(conf8);
		long Phase8EndTime = System.currentTimeMillis();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		date = new Date();
		System.out.println("[Measure]" + "Phase 8 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Count polyjoinpoint
		lines = readFile(hdfspath + ".polyjoinpoint/part-00000");
		if(!lines.isEmpty())
			totalNum = lines.get(0).split("\t");
		else{
			//System.out.println("join result is 0");
			totalNum[1] = "0";
		}
		double kij = Double.parseDouble(totalNum[1]);

		double Ld = (Ad * kij) / (3.14 * n1 * n2);
		//System.out.println("kij is " + kij);
		Ld = Math.sqrt(Ld) - Double.parseDouble(distance);
		System.out.println("ld is " + Ld);
		// Calculate n;
		long Phase0Time = (Phase0EndTime - ProjectStartTime)/1000;
		long Phase1Time = (Phase1EndTime - Phase0EndTime)/1000;
		long Phase2Time = (Phase2EndTime - Phase1EndTime)/1000;
		long Phase3Time = (Phase3EndTime - Phase2EndTime)/1000;
		long Phase4Time = (Phase4EndTime - Phase3EndTime)/1000;
		long Phase5Time = (Phase5EndTime - Phase4EndTime)/1000;
		long Phase6Time = (Phase6EndTime - Phase5EndTime)/1000;
		long Phase7Time = (Phase7EndTime - Phase6EndTime)/1000;
		long Phase8Time = (Phase8EndTime - Phase7EndTime)/1000;
		long ProjectRunningTIme = (Phase8EndTime - ProjectStartTime)/1000;
		String PhaseTimeStr = Phase0Time + "\t" + Phase1Time + "\t" + Phase2Time + "\t" + Phase3Time + "\t" + Phase4Time + "\t" + Phase5Time + "\t" + Phase6Time + "\t" + Phase7Time + "\t" + Phase8Time;
		PrintWriter ExecuteTimeWriter = null;
		try {
			ExecuteTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("ExecuteTime.txt", true)));
			ExecuteTimeWriter.println(PhaseTimeStr);
			ExecuteTimeWriter.close();
		} catch (IOException e ){

		}
		PrintWriter totalTimeWriter = null;
		try{
			totalTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("TotalTime.txt", true)));
			totalTimeWriter.println(ProjectRunningTIme);
			totalTimeWriter.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		System.out.println("Phase0 running time is: " + Phase0Time);
		System.out.println("Phase1 running time is: " + Phase1Time);
		System.out.println("Phase2 running time is: " + Phase2Time);
		System.out.println("Phase3 running time is: " + Phase3Time);
		System.out.println("Phase4 running time is: " + Phase4Time);
		System.out.println("Phase5 running time is: " + Phase5Time);
		System.out.println("Phase6 running time is: " + Phase6Time);
		System.out.println("Phase7 running time is: " + Phase7Time);
		System.out.println("Phase8 running time is: " + Phase8Time);
		System.out.println("Project running time is: " + ProjectRunningTIme
				+ "s");

	}

	public static void main(String args[]) throws IOException {
		Colocation colocation = new Colocation(args[0], args[1], args[2], args[3]);
		//System.out.println("distance is " + Colocation.distance);
		colocation.run();
	}
}