package com.jy.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MOTestMR extends Configured implements Tool {

    public static class MOMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, one);
        }
    }

    public static class MOReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        protected void reduce(Text Key, Iterable<IntWritable> Values, Context context) throws IOException, InterruptedException {
            // 开始位置
            int begin = Key.toString().indexOf("@");
            // 结束位置
            int end = Key.toString().indexOf(".");

            if (begin >= end) {
                return;
            }

            // 获取邮箱类别，比如 qq
            String name = Key.toString().substring(begin + 1, end);

            int sum = 0;
            for (IntWritable value : Values) {
                sum += value.get();
            }

            /*
             * multipleOutputs.write(key, value, baseOutputPath)方法的第三个函数表明了该输出所在的目录（相对于用户指定的输出目录）。
             * 如果baseOutputPath不包含文件分隔符"/"，那么输出的文件格式为baseOutputPath-r-nnnnn（name-r-nnnnn)；
             * 如果包含文件分隔符"/"，例如baseOutputPath="029070-99999/1901/part"，那么输出文件则为029070-99999/1901/part-r-nnnnn
             */
            multipleOutputs.write(Key, new IntWritable(sum), name);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @SuppressWarnings("deprecation")

    public int run(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();

        // 判断目录是否存在，如果存在，则删除
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        // 新建一个任务
        Job job = new Job(conf, "MultipleDemo");
        // 主类
        job.setJarByClass(MOTestMR.class);

        // 输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Mapper
        job.setMapperClass(MOMapper.class);
        // Reducer
        job.setReducerClass(MOReducer.class);

        // key输出类型
        job.setOutputKeyClass(Text.class);
        // value输出类型
        job.setOutputValueClass(IntWritable.class);

        // 去掉job设置outputFormatClass，改为通过LazyOutputFormat设置
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 数据输入路径和输出路径
        args = new String[] {
                "D:\\mr_file\\input",
                "D:\\mr_file\\output"
        };
        int ec = ToolRunner.run(new Configuration(), new MOTestMR(), args);
        System.exit(ec);
    }
}
