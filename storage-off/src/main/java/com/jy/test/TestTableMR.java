package com.jy.test;

import com.jy.mr.BinlogPartitionMR;
import com.jy.util.HadoopTool;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TestTableMR extends HadoopTool {

    public static void main(String[] args) throws Exception {
        execMain(new TestTableMR(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = getJobInstance(configuration);

        job.setJarByClass(TestTableMR.class);

        job.setMapperClass(TestTableMRMapper.class);
        job.setReducerClass(TestTableMRReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TestTableMRMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        Text outputKey = new Text();
        NullWritable outputValue = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = JSONObject.fromObject(value.toString());

            JSONObject jsonObjectHead = jsonObject.getJSONObject("head");

            String db = jsonObjectHead.getString("db");
            String table = jsonObjectHead.getString("table");

            outputKey.set(db + "." + table);

            context.write(outputKey, outputValue);
        }
    }

    public static class TestTableMRReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

}
