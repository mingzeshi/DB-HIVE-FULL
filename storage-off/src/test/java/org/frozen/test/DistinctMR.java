package org.frozen.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistinctMR extends Configured implements Tool {
	public static void main(String[] args) {
		args = new String[] {
				"D:/mr_file/20181025/input",
				"D:/mr_file/20181025/output"
		};
		
		try {
			ToolRunner.run(new DistinctMR(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration);
		job.setJarByClass(DistinctMR.class);
		job.setMapperClass(DistinctMRMap.class);
		job.setReducerClass(DistinctMRReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class DistinctMRMap extends Mapper<LongWritable, Text, Text, Text> {
		private static Text outputKey = new Text();
		private static Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(",");
			outputKey.set(words[0]);
			outputValue.set(words[1]);
			
			context.write(outputKey, outputValue);
		}
	}
	
	public static class DistinctMRReducer extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> valuesIterator = values.iterator();
			Text value = valuesIterator.next();

			outputValue.set(value);
			context.write(key, outputValue);
		}
	}
}
