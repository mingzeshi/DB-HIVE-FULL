package com.jy.mr.dataCleansing;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarkPointCleansingJSON extends Configured implements Tool {
	
	public static void main(String[] args) {
		try {
//			args = new String[] {
//					"C:\\Users\\Administrator\\Desktop\\文件\\20191114\\input_dir\\*",
//					"C:\\Users\\Administrator\\Desktop\\文件\\20191114\\output_dir\\"
//			};
			
			ToolRunner.run(new MarkPointCleansingJSON(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(MarkPointCleansingJSON.class);
		
		job.setMapperClass(MarkPointCleansingJSONMap.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
				
	}
	

	public static class MarkPointCleansingJSONMap extends Mapper<LongWritable, Text, NullWritable, Text> {

		String part_log_day_DIR = null;
		Pattern r = null;
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		NullWritable outputKey = NullWritable.get();
		Text outputValue = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();

			part_log_day_DIR = fileSplit.getPath().getParent().getName();

			String pattern = "(\\{[\\s\\S]*?\\})";

			r = Pattern.compile(pattern); // 创建 Pattern 对象
			
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String str = value.toString();

			Matcher m = r.matcher(str); // 现在创建 matcher 对象

			while (m.find()) {
				String old_str = m.group(1);

				String new_str = old_str.replace(",", "，");

				str = str.replace(old_str, new_str);
			}
			
			outputValue.set(str);

			multipleOutputs.write(outputKey, outputValue, part_log_day_DIR + "/");

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (multipleOutputs != null) {
				multipleOutputs.close();
			}
		}

	}
}
