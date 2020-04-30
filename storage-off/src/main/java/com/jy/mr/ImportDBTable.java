package com.jy.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.jy.bean.importDBBean.ImportDataBean;
import com.jy.bean.importDBBean.ImportRDBDataSetDB;
import com.jy.constant.Constants;
import com.jy.mr.datadrivendbinputformat.CustomDataDrivenDBInputFormat;
import com.jy.mr.datadrivendbinputformat.CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit;
import com.jy.mr.dbinputformat.CustomDBInputFormat;
import com.jy.mr.dbinputformat.CustomDBInputFormat.DBInputSplit;
import com.jy.util.DateUtils;
import com.jy.util.HadoopTool;
import com.jy.util.XmlUtil;

public class ImportDBTable extends HadoopTool {
	
	public static void main(String[] args) throws Exception {
        execMain(new ImportDBTable(), args);
    }
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf(); // 创建配置信息
		
		ImportRDBDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get("import.db.config.path"), "db"); // 获取配置文件数据库相关信息

		DBConfiguration.configureDB(configuration, dataSetDB.getDriver(), dataSetDB.getUrl(), dataSetDB.getUsername(), dataSetDB.getPassword()); // 通过conf创建数据库配置信息

//		FileSystem fileSystem = FileSystem.get(configuration); // 创建文件系统

		Job job = getJobInstance(configuration);

		job.setJarByClass(ImportDBTable.class);

//		String engine = "E1"; // 获取参数
		String engine = null; // 获取参数
		if(Constants.ENGINE1.equals(engine)) {
			job.setMapperClass(DBInputFormatMapper.class);
	
			job.setInputFormatClass(CustomDBInputFormat.class);
			CustomDBInputFormat.setInput(job, ImportDataBean.class);		
		} else { // 默认执行引擎
			job.setMapperClass(DataDrivenDBInputFormatMapper.class);

			job.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
			CustomDataDrivenDBInputFormat.setInput(job, ImportDataBean.class);
		}

//		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0] + "/" + DateUtils.getYesterdayDate() + "/" + dataSetDB.getEnname() + "/"));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @描述：使用DBInputFormat组件Import数据
	 * @author Administrator
	 *
	 */
	public static class DBInputFormatMapper extends Mapper<LongWritable, ImportDataBean, NullWritable, Text> {
		 private NullWritable mapOutKey = NullWritable.get();
		 private Text mapOutValue = new Text();
		 
		 private MultipleOutputs<NullWritable, Text> multipleOutputs;
		 DBInputSplit dbSplit;
		 String outputPath;
		 String importDB;
		 String importTable;
		 
		 @Override
		 protected void setup(Context context) throws IOException, InterruptedException {
			 multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			 dbSplit = (DBInputSplit) context.getInputSplit();

			 importDB = dbSplit.getDb();
			 importTable = dbSplit.getTable();

			 outputPath = importTable + "/" + importTable;
		 }
		 
		@Override
		protected void map(LongWritable key, ImportDataBean value, Context context) throws IOException, InterruptedException {
			value.setDb(importDB);
			value.setTable(importTable);
			
			mapOutValue.set(value.getData());
			
			multipleOutputs.write(mapOutKey, mapOutValue, outputPath);
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, ImportDataBean, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			if(multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}
	
	/**
	 * @描述：使用DataDrivenDBInputFormat组件Import数据
	 * @author Administrator
	 *
	 */
	public static class DataDrivenDBInputFormatMapper extends Mapper<LongWritable, ImportDataBean, Text, Text> {
		 private Text mapOutKey = new Text();
		 private Text mapOutValue = new Text();
		 
		 private MultipleOutputs<Text, Text> multipleOutputs;
		 DataDrivenDBInputSplit dbSplit;
		 String outputPath;
		 String importDB;
		 String importTable;
		 String outputformatDir;
		 
		 @Override
		 protected void setup(Context context) throws IOException, InterruptedException {
			 Configuration configuration = context.getConfiguration();
			 
			 multipleOutputs = new MultipleOutputs<Text, Text>(context);
			 dbSplit = (DataDrivenDBInputSplit) context.getInputSplit();
			 
			 importDB = dbSplit.getDb();
			 importTable = dbSplit.getTable();

			 outputPath = importTable + "/" + importTable;
		 }
		 
		@Override
		protected void map(LongWritable key, ImportDataBean value, Context context) throws IOException, InterruptedException {
			
			value.setDb(importDB);
			value.setTable(importTable);
			
			mapOutKey.set(importDB + Constants.SPECIALCHAR + importTable);
			mapOutValue.set(value.getTitle() + Constants.KEY_VALUE_JOIN + value.getData());
			
			multipleOutputs.write(mapOutKey, mapOutValue, outputPath);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}
}