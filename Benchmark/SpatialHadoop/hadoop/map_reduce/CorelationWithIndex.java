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
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CorelationWithIndex {
	private final static LongWritable one = new LongWritable(1);
	public static long totalNumOfRecord = 0;
	public static Double avgEcoIndex = 0.0;
	public static long SumEcoIndex = 0;
	private static Text word = new Text();
	public static String inputRec1;
	public static String inputRec2;
	public static String outputRec;
	public static String namenode = "hdfs://master:9000";
	public static String hdfspath = "";

	public CorelationWithIndex(String hdfs, String inputRec1) {
		this.inputRec1 = inputRec1;

		this.hdfspath = hdfs;
	}

	// Map Class to Count the total Number of Records.
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

	public static class MapForSum extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split("\t");
			long count = Long.parseLong(token[1]);
			output.collect(word, new LongWritable(count));
		}
	}

	public static class ReduceForSum extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
		}
	}

	// This MapReduce will use (key - avg)*(val - avg)
	public static class MapForDom extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split("\t");
			long xi = Long.parseLong(token[0]);
			long xy = Long.parseLong(token[1]);
			double result = (xi - avgEcoIndex) * (xy - avgEcoIndex);
			output.collect(word, new DoubleWritable(result));
		}
	}

	public static class ReduceForDom extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			double sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new DoubleWritable(sum));
		}
	}

	public static class MapForNom extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String token[] = line.split("\t");
			double xi = Double.parseDouble(token[1]);
			double result = (xi - avgEcoIndex) * (xi - avgEcoIndex);
			output.collect(word, new DoubleWritable(result));
		}
	}

	public static class ReduceForNom extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			double sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new DoubleWritable(sum));
		}
	}

	private static void deletehdfsFile(String path) throws IOException {
		URI uri = URI.create(path);
		Path pathhadoop = new Path(uri);
		Configuration confhadoop = new Configuration();
		FileSystem filehadoop = FileSystem.get(uri, confhadoop);
		filehadoop.delete(pathhadoop, true);
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

	public static void copyFile(String file) {
		try {
			deletehdfsFile(file + ".copy");
			Runtime rt = Runtime.getRuntime();
			Process pr = rt
					.exec("hadoop fs -cp " + file + " " + file + ".copy");
			pr.waitFor();
		} catch (Exception e) {
		}
	}

	public static void delete(String file) {
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec("hadoop fs -rmr " + file);
			pr.waitFor();
		} catch (Exception e) {
		}
	}

	public void run() throws IOException{
		PrintWriter phaseTimeWriter = null; 
		delete(inputRec1+".copy");
		copyFile(inputRec1);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("[Measure]" + "Project begin at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		long ProjectStartTime = System.currentTimeMillis();
		
		// First MapReduce phase count the total number of records;
		JobConf conf1 = new JobConf(CorelationWithIndex.class);
		conf1.setJobName("Count records");
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);

		conf1.setMapperClass(MapForCount.class);
		conf1.setCombinerClass(ReduceForCount.class);
		conf1.setReducerClass(ReduceForCount.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(conf1, new Path(hdfspath + inputRec1));
		FileOutputFormat.setOutputPath(conf1, new Path(hdfspath
				+ ".countRecord"));
		deletehdfsFile(hdfspath + ".countRecord");
		JobClient.runJob(conf1);
		long Phase0EndTime   = System.currentTimeMillis();
		long Phase0Time = (Phase0EndTime - ProjectStartTime)/1000;
		System.out.println("[Measure]"+"Running Time for MapReduce job 1: " +Phase0Time);
		date = new Date();
		System.out.println("[Measure]" + "Phase 1 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		delete(hdfspath + "aggtemp");
		// Second MapReduce Job, Calculate total sum of ecoIndex
		JobConf conf2 = new JobConf(CorelationWithIndex.class);
		conf2.setJobName("Sum");
		conf2.setMapperClass(MapForSum.class);
		conf2.setCombinerClass(ReduceForSum.class);
		conf2.setReducerClass(ReduceForSum.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(conf2, new Path(hdfspath + inputRec1));
		FileOutputFormat.setOutputPath(conf2, new Path(hdfspath + ".sum"));
		delete(hdfspath + ".sum");
		

		JobClient.runJob(conf2);
		long Phase1EndTime   = System.currentTimeMillis();
		long Phase1Time = (Phase1EndTime - Phase0EndTime)/1000;
		System.out.println("[Measure]"+"Running Time for MapReduce job 2: " +Phase1Time);
		date = new Date();
		System.out.println("[Measure]" + "Phase 2 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		// Calculate the average, read from output of the first and second
		// map-reduce job.

		ArrayList<String> lines = readFile(hdfspath + ".sum/part-00000");
		String[] totalNum = lines.get(0).split("\t");
		SumEcoIndex = Long.parseLong(totalNum[1]);

		lines = readFile(hdfspath + ".countRecord/part-00000");
		totalNum = lines.get(0).split("\t");
		totalNumOfRecord = Long.parseLong(totalNum[1]);

		avgEcoIndex = (double) (SumEcoIndex) / (double) totalNumOfRecord;
		System.out.println(" Total is " + totalNumOfRecord + "\n Sum is "
				+ SumEcoIndex + "\n Avg is " + avgEcoIndex);
		

		// Third MapReduce Job, SelfJoin. This will take <Rectangle, EcoIndex>
		// as input and output <EcoIndex, EcoIndex>
		try {
			String shadoop = "shadoop index "
					+ inputRec1 + " "+ inputRec1+".index" + " "
					+ "sindex:rtree" + " -overwrite " + "shape:edu.umn.cs.spatialHadoop.core.Pair";

			Process p = Runtime.getRuntime().exec(shadoop);
			InputStream stderr = p.getErrorStream();
			InputStream stdout = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			InputStreamReader iso = new InputStreamReader(stdout);
			BufferedReader br = new BufferedReader(isr);
			String line1, line2 = null;
			while ( (line1 = br.readLine()) != null || (line2 = br.readLine()) != null){
				if(line1 != null)
					System.err.println(line1);
				if(line2 != null)
					System.err.println(line1);
			}
			p.waitFor();
					/*Shell.execCommand("shadoop sjmr "
					+ inputRec1 + " "+ inputRec1+".copy" + " "
					+ "joinResult" + " -overwrite " + "shape:edu.umn.cs.spatialHadoop.core.Pair");*/
			//p.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			String shadoop = "shadoop index "
					+ inputRec1+".copy" + " "+ inputRec1+".copy.index" + " "
					+ "sindex:rtree" + " -overwrite " + "shape:edu.umn.cs.spatialHadoop.core.Pair";

			Process p = Runtime.getRuntime().exec(shadoop);
			InputStream stderr = p.getErrorStream();
			InputStream stdout = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			InputStreamReader iso = new InputStreamReader(stdout);
			BufferedReader br = new BufferedReader(isr);
			String line1, line2 = null;
			while ( (line1 = br.readLine()) != null || (line2 = br.readLine()) != null){
				if(line1 != null)
					System.err.println(line1);
				if(line2 != null)
					System.err.println(line1);
			}
			p.waitFor();
					/*Shell.execCommand("shadoop sjmr "
					+ inputRec1 + " "+ inputRec1+".copy" + " "
					+ "joinResult" + " -overwrite " + "shape:edu.umn.cs.spatialHadoop.core.Pair");*/
			//p.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			String shadoop = "shadoop sjmr "
					+ inputRec1+".index" + " "+ inputRec1+".copy.index" + " "
					+ "joinResult" + " -overwrite " + "shape:edu.umn.cs.spatialHadoop.core.Pair";
			
			Process p = Runtime.getRuntime().exec(shadoop);
			InputStream stderr = p.getErrorStream();
			InputStream stdout = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(stderr);
            InputStreamReader iso = new InputStreamReader(stdout);
            BufferedReader br = new BufferedReader(isr);
            String line1, line2 = null;
            while ( (line1 = br.readLine()) != null || (line2 = br.readLine()) != null){
            	if(line1 != null)
            		System.err.println(line1);
            	if(line2 != null)
            		System.err.println(line1);
            }
			p.waitFor();
					/*Shell.execCommand("shadoop sjmr "
					+ inputRec1 + " "+ inputRec1+".copy" + " "
					+ "joinResult" + " -overwrite " + "shape:edu.umn.cs.spatialHadoop.core.Pair");*/
			//p.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long Phase2EndTime   = System.currentTimeMillis();
		long Phase2Time = (Phase2EndTime - Phase1EndTime)/1000;
		System.out.println("[Measure]"+"Running Time for MapReduce job 3: " +Phase2Time);
		date = new Date();
		System.out.println("[Measure]" + "Phase 3 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		deletehdfsFile(hdfspath + "aggtemp");
		// Fourth MapReduce Job, calculate the dominator part
		// n*\Sigma*w_ij*(x_i-\bar{x})(xj - \bar{x})
		JobConf conf4 = new JobConf(CorelationWithIndex.class);
		conf4.setJobName("DOM");
		conf4.setOutputKeyClass(Text.class);
		conf4.setOutputValueClass(IntWritable.class);

		conf4.setMapperClass(MapForDom.class);
		conf4.setCombinerClass(ReduceForDom.class);
		conf4.setReducerClass(ReduceForDom.class);

		conf4.setInputFormat(TextInputFormat.class);
		conf4.setOutputKeyClass(Text.class);
		conf4.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(conf4, new Path(hdfspath + "joinResult"));
		FileOutputFormat.setOutputPath(conf4, new Path(hdfspath + ".dom"));
		deletehdfsFile(hdfspath + ".dom");

		JobClient.runJob(conf4);
		long Phase3EndTime   = System.currentTimeMillis();
		long Phase3Time = (Phase3EndTime - Phase2EndTime)/1000;
		System.out.println("[Measure]"+"Running Time for MapReduce job 4: " +Phase3Time);
		date = new Date();
		System.out.println("[Measure]" + "Phase 4 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		deletehdfsFile(hdfspath + "aggtemp");
		// Read the result from the fourth map-reduce job.
		lines = readFile(hdfspath + ".dom/part-00000");
		totalNum = lines.get(0).split("\t");
		double dom = Double.parseDouble(totalNum[1]);

		// Fifth Job, Calculate Nom
		JobConf conf5 = new JobConf(CorelationWithIndex.class);
		conf5.setJobName("Nom");
		conf5.setOutputKeyClass(Text.class);
		conf5.setOutputValueClass(IntWritable.class);

		conf5.setMapperClass(MapForNom.class);
		conf5.setCombinerClass(ReduceForNom.class);
		conf5.setReducerClass(ReduceForNom.class);

		conf5.setInputFormat(TextInputFormat.class);
		conf5.setOutputKeyClass(Text.class);
		conf5.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(conf5, new Path(hdfspath + inputRec1));
		FileOutputFormat.setOutputPath(conf5, new Path(hdfspath + ".nom"));
		deletehdfsFile(hdfspath + ".nom");

		JobClient.runJob(conf5);
		long Phase4EndTime   = System.currentTimeMillis();
		long Phase4Time = (Phase4EndTime - Phase3EndTime)/1000;
		System.out.println("[Measure]"+"Running Time for MapReduce job 5: " +Phase4Time);
		date = new Date();
		System.out.println("[Measure]" + "Phase 5 finish at "
				+ dateFormat.format(date));
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		deletehdfsFile(hdfspath + "aggtemp");
		// Six Calculate Wij
		JobConf conf6 = new JobConf(CorelationWithIndex.class);
		conf6.setJobName("Wij");
		conf6.setOutputKeyClass(Text.class);
		conf6.setOutputValueClass(IntWritable.class);

		conf6.setMapperClass(MapForCount.class);
		conf6.setCombinerClass(ReduceForCount.class);
		conf6.setReducerClass(ReduceForCount.class);

		conf6.setInputFormat(TextInputFormat.class);
		conf6.setOutputKeyClass(Text.class);
		conf6.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(conf6, new Path(hdfspath + "joinResult"));
		FileOutputFormat.setOutputPath(conf6, new Path(hdfspath + ".wij"));
		deletehdfsFile(hdfspath + ".wij");
		JobClient.runJob(conf6);
		long Phase5EndTime   = System.currentTimeMillis();
		long Phase5Time = (Phase5EndTime - Phase4EndTime)/1000;
		System.out.println("[Measure]"+"Running Time for MapReduce job 6: " +Phase5Time);
		date = new Date();
		try {
			phaseTimeWriter = new PrintWriter(new BufferedWriter(new FileWriter("PhaseTime.txt", true)));
			phaseTimeWriter.println(dateFormat.format(date));
			phaseTimeWriter.close();
		} catch(IOException e) {

		}
		System.out.println("[Measure]" + "Phase 6 finish at "
				+ dateFormat.format(date));
		deletehdfsFile(hdfspath + "aggtemp");
		// Read from wij
		lines = readFile(hdfspath + ".wij/part-00000");
		totalNum = lines.get(0).split("\t");
		double wij = Double.parseDouble(totalNum[1]);
		// Calculate I
		lines = readFile(hdfspath + ".nom/part-00000");
		totalNum = lines.get(0).split("\t");
		double nom = Double.parseDouble(totalNum[1]);

		double I = (totalNumOfRecord * dom) / (wij * nom);
//		System.out.println("Total is " + totalNumOfRecord + "\nSum is "
//				+ SumEcoIndex + "\nAvg is " + avgEcoIndex);
//		System.out.println("TotalNumOfRecord  is " + totalNumOfRecord);
//		System.out.println("Nom  is " + nom);
//		System.out.println("Dom is " + dom);
//		System.out.println("wij is " + wij);
//		System.out.println("[Measure]"+"I is " + I);
		
		long ProjectEndTime = System.currentTimeMillis();
		long ProjectRunningTIme = (ProjectEndTime - ProjectStartTime)/1000;
		System.out.println("[Measure]"+"Running Time for Whole MapReduce job: " +ProjectRunningTIme);

		String PhaseTimeStr = Phase0Time + "\t" + Phase1Time + "\t" + Phase2Time + "\t" + Phase3Time + "\t" + Phase4Time + "\t" + Phase5Time;
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
	}

	public static void main(String args[]) throws IOException {
		CorelationWithIndex ac = new CorelationWithIndex(args[0], args[1]);
		ac.run();
	}
}

