package org.frozen.test.other;

import java.io.IOException;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JsonDBTable extends Configured implements Tool {
	
	public static void main(String[] args) {
		args = new String[] {
				"D:/binlog/input",
				"D:/binlog/output"
		};
		
		try {
			ToolRunner.run(new JsonDBTable(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration);
		job.setJarByClass(JsonDBTable.class);
		job.setMapperClass(JMap.class);
		job.setReducerClass(JReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class JMap extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		String ctime = "create_time";
		String cdate = "create_date";
		String cDate = "createDate";
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			JSONObject jsonObject = JSONObject.fromObject(value.toString());

            JSONObject jsonObjectHead = jsonObject.getJSONObject("head");

            String db = jsonObjectHead.getString("db");
            String table = jsonObjectHead.getString("table");
            
            outputKey.set(db + "." + table);
            
            if(value.toString().contains(ctime)) {
            	outputValue.set(ctime);
            } else if(value.toString().contains(cdate)) {
            	outputValue.set(cdate);
            } else if(value.toString().contains(cDate)) {
            	outputValue.set(cDate);
            } else {
            	outputValue.set("-");
            }
            
            context.write(outputKey, outputValue);
		}
	}
	
	
	public static class JReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}
}
