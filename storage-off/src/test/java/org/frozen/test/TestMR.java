package org.frozen.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.frozen.util.HadoopTool;

public class TestMR extends HadoopTool {

    public static void main(String[] args) throws Exception {
        execMain(new TestMR(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = getJobInstance(configuration);

        job.setJarByClass(TestMR.class);

        job.setMapperClass(TestMRMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TestMRMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        NullWritable outputKey = NullWritable.get();
        Text outputValue = new Text();
        StringBuffer buf = new StringBuffer();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\u0001");

            for(String str : strs) {
                buf.append(str + "\t");
            }

            buf.substring(0, buf.length() - 1);

            outputValue.set(buf.toString());
            context.write(outputKey, outputValue);
            buf.delete(0, buf.length());
        }
    }

}
