package org.frozen.test.other;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.frozen.bean.importDBBean.ImportDataBean;
import org.frozen.test.custom.DataDrivenDBInputFormatC;

public class TestDBDriven extends Configured implements Tool {
	
	public static void main(String[] args) {
		args = new String[] {
				"D:/mr_file/20190211/output_testDriven",
		};
		
        try {
			ToolRunner.run(new TestDBDriven(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
	@Override
	public int run(String[] args) throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://47.101.78.126:13306/wind";
		String username = "touyan";
		String password = "4ELaAzte";
		
		Configuration configuration = new Configuration(); // 创建配置信息
		configuration.setInt("mapreduce.job.maps", 5);

		DBConfiguration.configureDB(configuration, driver, url, username, password); // 通过conf创建数据库配置信息

		Job job = Job.getInstance(configuration);

		job.setJarByClass(TestDBDriven.class);

		job.setMapperClass(TestDBDrivenMapper.class);

		job.setInputFormatClass(DataDrivenDBInputFormatC.class);
		DataDrivenDBInputFormatC.setInput(job, ImportDataBean.class, "ASHAREPREVIOUSENNAME", null, "object_id", "*");

		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class TestDBDrivenMapper extends Mapper<LongWritable, ImportDataBean, Text, Text> {
		private Text mapOutKey = new Text();
		private Text mapOutValue = new Text();
	 
		@Override
		protected void map(LongWritable key, ImportDataBean value, Context context) throws IOException, InterruptedException {

			mapOutKey.set(value.getTitle());
			mapOutValue.set(value.getData());
			
			context.write(mapOutKey, mapOutValue);
		}
	}
}