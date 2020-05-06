package org.frozen.test.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import org.frozen.constant.Constants;

public class TestORC {
	public static class OrcMap extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {
		
		NullWritable outputKey = NullWritable.get();
		Text outputValue = new Text();

		public void map(NullWritable key, OrcStruct value, Context output) throws IOException, InterruptedException {
			int fieldsNumber = value.getNumFields();

			StringBuffer buf = new StringBuffer();
			for(int i = 0; i < fieldsNumber; i++) {
				buf.append((value.getFieldValue(i) == null ? "" : value.getFieldValue(i).toString()) + Constants.SPECIALCHAR);
			}
			
			outputValue.set(buf.substring(0, buf.length() - 1));
			output.write(outputKey, outputValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(TestORC.class);
		job.setJobName("parquetthrfit");

		String in = "D:/mr_file/20190211/orc_file/input";
		String out = "D:/mr_file/20190211/orc_file/output";

		job.setMapperClass(OrcMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(OrcInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		OrcInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
	}

}
