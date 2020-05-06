package org.frozen.test.sequence;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.beanutils.locale.converters.LongLocaleConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestSequenceFileRead  extends Configured implements Tool {
	
	public static void main(String[] args) {
		
		args = new String[] {
				"D:/mr_file/20190218/write_sequence_output",
				"D:/mr_file/20190218/read_sequence_output"
		}; 
		
		try {
			ToolRunner.run(new TestSequenceFileRead(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TestSequenceFileRead.class);

        job.setMapperClass(SFMapper.class);
        job.setReducerClass(SFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VLongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VLongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class SFMapper extends Mapper<BytesWritable, BytesWritable, Text, VLongWritable> {
		Text outputKey = new Text();
		VLongWritable outputValue = new VLongWritable();
		
		public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			byte[] keyBytes = key.getBytes();
			keyBytes = Arrays.copyOfRange(keyBytes, 0, key.getLength());
			
			byte[] valueBytes = value.getBytes();
			valueBytes = Arrays.copyOfRange(valueBytes, 0, value.getLength());
			
			String strKey = new String(keyBytes);
			String strValue = new String(valueBytes);
			
			outputKey.set(strKey);
			outputValue.set(Long.valueOf(strValue));
			
			context.write(outputKey, outputValue);
		}
	}

	// reduce
	public static class SFReducer extends Reducer<Text, VLongWritable, Text, VLongWritable> {
		@Override
		protected void reduce(Text key, Iterable<VLongWritable> v2s, Context context) throws IOException, InterruptedException {
			for (VLongWritable vl : v2s) {
				context.write(key, vl);
			}
		}
	}
}
