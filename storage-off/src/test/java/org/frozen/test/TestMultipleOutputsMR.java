package org.frozen.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestMultipleOutputsMR extends Configured implements Tool {
	
	public static void main(String[] args) {
		args = new String[] {
				"D:/mr_file/20190211/text_file/input/",
				"D:/mr_file/20190211/text_file/output/"
		};
		
		try {
			ToolRunner.run(new TestMultipleOutputsMR(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(TestMultipleOutputsMR.class);
		
		job.setMapperClass(TestMultipleOutputsMRMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class TestMultipleOutputsMRMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		private MultipleOutputs<Text, Text> multipleOutputs;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			outputKey.set(key.toString());
			outputValue.set(value.toString());
			
			multipleOutputs.write(outputKey, outputValue, "D:/mr_file/20190211/text_file/aa/cc/");
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}

}
