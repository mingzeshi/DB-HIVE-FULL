package org.frozen.test.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

public class TestORCWrite {
	
	// OrcStruct
	public static class OrcWriterMapper extends Mapper<LongWritable, Text, NullWritable, OrcStruct> {

		private TypeDescription schema = TypeDescription.fromString("struct<userid:string,jlc_userid:string,reg_time:string,open_time:string,invest1st_time:string,invest1st_amount:double,invest2st_time:string,invest2st_amount:double,invest_num:double,invest_amount:double,planing_userid:string,start1rd_time:string,end1rd_time:string,plan1st_time:string,plan1st_amount:double,redeem1st_time:string,redeem1st_amount:double,redeem_num:double,redeem_amount:double,user_realname:string,risk_rate:string,gender:string,age:double,identity_number:string,phone:string,identity_number_en:string,phone_en:string,province:string,city:string,born_address:string,source:string,channel:string,bank_name:string,premium_max:double,premium_current:double,availableamt_current:double,investlast_time:string,investlast_amount:double,redeemlast_time:string,redeemlast_amount:double,income_current:double,income_total:double,rate:string>");
		private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

		private final NullWritable outputKey = NullWritable.get();
//		private Text outputValue = new Text();

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

			String[] arr = value.toString().split(",");

			for(int i = 0; i < arr.length; i++) {
				Text outputValue = new Text();
				outputValue.set(arr[i]);
				pair.setFieldValue(i, outputValue);					
			}
			output.write(outputKey, pair);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, "struct<userid:string,jlc_userid:string,reg_time:string,open_time:string,invest1st_time:string,invest1st_amount:double,invest2st_time:string,invest2st_amount:double,invest_num:double,invest_amount:double,planing_userid:string,start1rd_time:string,end1rd_time:string,plan1st_time:string,plan1st_amount:double,redeem1st_time:string,redeem1st_amount:double,redeem_num:double,redeem_amount:double,user_realname:string,risk_rate:string,gender:string,age:double,identity_number:string,phone:string,identity_number_en:string,phone_en:string,province:string,city:string,born_address:string,source:string,channel:string,bank_name:string,premium_max:double,premium_current:double,availableamt_current:double,investlast_time:string,investlast_amount:double,redeemlast_time:string,redeemlast_amount:double,income_current:double,income_total:double,rate:string>");
		conf.set("orc.mapred.output.schema", "struct<userid:string,jlc_userid:string,reg_time:string,open_time:string,invest1st_time:string,invest1st_amount:double,invest2st_time:string,invest2st_amount:double,invest_num:double,invest_amount:double,planing_userid:string,start1rd_time:string,end1rd_time:string,plan1st_time:string,plan1st_amount:double,redeem1st_time:string,redeem1st_amount:double,redeem_num:double,redeem_amount:double,user_realname:string,risk_rate:string,gender:string,age:double,identity_number:string,phone:string,identity_number_en:string,phone_en:string,province:string,city:string,born_address:string,source:string,channel:string,bank_name:string,premium_max:double,premium_current:double,availableamt_current:double,investlast_time:string,investlast_amount:double,redeemlast_time:string,redeemlast_amount:double,income_current:double,income_total:double,rate:string>");

		Job job = Job.getInstance(conf);
		job.setJarByClass(TestORCWrite.class);
		job.setJobName("OrcWriterMR");

		String in = "D:/mr_file/20190211/textToORC/input";
		String out = "D:/mr_file/20190211/textToORC/output";

		job.setMapperClass(OrcWriterMapper.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(OrcStruct.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(OrcOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(in));
		OrcOutputFormat.setOutputPath(job, new Path(out));

		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
	}

}
