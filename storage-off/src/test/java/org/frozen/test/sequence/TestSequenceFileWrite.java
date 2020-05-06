package org.frozen.test.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestSequenceFileWrite extends Configured implements Tool {
	
	public static void main(String[] args) {
		args = new String[] {
				"D:/mr_file/20190218/write_sequence_input",
				"D:/mr_file/20190218/write_sequence_output"
		};
		
		try {
			ToolRunner.run(new TestSequenceFileWrite(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TestSequenceFileWrite.class);

		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VLongWritable.class);

		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);

		// 设置输出类
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		/**
		 * 设置sequecnfile的格式，对于sequencefile的输出格式，有多种组合方式, 从下面的模式中选择一种，并将其余的注释掉
		 */

		// 组合方式1：不压缩模式
		// SequenceFileOutputFormat.setOutputCompressionType(job,
		// CompressionType.NONE);

		// 组合方式2：record压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.RECORD);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

		// 组合方式3：block压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

		FileInputFormat.addInputPaths(job, args[0]);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	//map
	public static class WCMapper extends Mapper<LongWritable, Text, Text, VLongWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			
			for(String str : strs) {
				context.write(new Text(str), new VLongWritable(1L));
			}
		}
	}

    //reduce
	public static class WCReducer extends Reducer<Text, VLongWritable, BytesWritable, BytesWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<VLongWritable> v2s, Context context) throws IOException, InterruptedException {

			long sum = 0;

			for (VLongWritable vl : v2s) {
				sum += vl.get();
			}

			System.out.println(key + ":" + sum);
			context.write(new BytesWritable(key.getBytes()), new BytesWritable(String.valueOf(sum).getBytes()));
		}
	}
}
