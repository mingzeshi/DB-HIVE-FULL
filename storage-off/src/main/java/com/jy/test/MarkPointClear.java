package com.jy.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MarkPointClear extends Configured implements Tool {
	
	public static void main(String[] args) {
		args = new String[] {
				"C:/Users/Administrator/Desktop/文件/20190718/*/",
				"C:/Users/Administrator/Desktop/文件/20190718/mark_output"
		};
		
		try {
			ToolRunner.run(new MarkPointClear(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(MarkPointClear.class);
		
		job.setMapperClass(MarkPointClearMap.class);
		job.setReducerClass(MarkPointClearReduct.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class MarkPointClearMap extends Mapper<LongWritable, Text, Text, Text> {
		private static Text outputKey = new Text();
		private static Text outputValue = new Text();
		
		
		
		String fileName = "";
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			System.out.println("========================================" + fileName);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		}
	}
	
	public static class MarkPointClearReduct extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		private static NullWritable outputKey = NullWritable.get();
		private static Text outputValue = new Text();
		
		@Override
		protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext()) {
				outputValue.set(iterator.next().toString());
				multipleOutputs.write(outputKey, outputValue, "");
			}
		}
	}

}
