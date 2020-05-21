package org.frozen.test.other;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WC extends Configured implements Tool {

	public static void main(String[] args) {
		args = new String[] {
				"-DconfigFile=xxx",
				"-Dtable=user_info",
//				"D:/mr_file/20190130/wc_data/input",
//				"D:/mr_file/20190130/wc_data/output"
				"/files/",
				"/smz_output"
		};
		
		try {
			ToolRunner.run(new WC(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://10.103.27.164:8020");

//		FileSystem fileSystem = FileSystem.get(configuration);
//		fileSystem.copyFromLocalFile(new Path(""), new Path(""));
		
		GenericOptionsParser parser = new GenericOptionsParser(configuration, args);
        String[] toolArgs = parser.getRemainingArgs();
        
        String f = configuration.get("configFile");
        String t = configuration.get("table");
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(WC.class);
		job.setMapperClass(WCMap.class);
		job.setReducerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(toolArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(toolArgs[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class WCMap extends Mapper<LongWritable, Text, Text, LongWritable> {
		private static Text outputKey = new Text();
		private static LongWritable outputValue = new LongWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\t");
			for(String word : words) {
				outputKey.set(word);
				
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private MultipleOutputs<Text, LongWritable> multipleOutputs;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, LongWritable>(context);
		}
		
		private LongWritable outputValue = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Long sum = 0L;
			Iterator<LongWritable> valuesIterator = values.iterator();
			while(valuesIterator.hasNext()) {
				sum += valuesIterator.next().get();
			}
			
			outputValue.set(sum);
//			context.write(key, outputValue);
			
			multipleOutputs.write("q", key, outputValue, "aa/bb/");
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}
}
