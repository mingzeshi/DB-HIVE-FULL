package com.jy.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.jy.bean.loadHiveBean.HiveDataBase;
import com.jy.bean.loadHiveBean.HiveMetastore;
import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import com.jy.constant.Constants;
import com.jy.mr.sequenceinputformat.OWFileSplit;
import com.jy.mr.sequenceinputformat.SequenceFileInputFormatOWInputSplit;
import com.jy.util.DateUtils;
import com.jy.util.HadoopTool;
import com.jy.util.HadoopUtil;
import com.jy.util.JedisOperation;
import com.jy.util.XmlUtil;

public class HdfsToHive extends HadoopTool  {
	
	public static void main(String[] args) throws Exception {
        execMain(new HdfsToHive(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf(); // 创建配置信息
		FileSystem fileSystem = FileSystem.get(configuration);

		// ---------------------------------------------------------------------------------------

		HiveMetastore hiveDWMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(configuration.get("hdfs.load.hive.dw.path"), "db"); // 获取近源数据快照层配置文件相关信息
		List<HiveDataBase> dwdataBaseList = hiveDWMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveDWDataSet> dwdataBase = dwdataBaseList.get(0); // 获取hive-db的location 
		String hive_dw_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, dwdataBase.getEnnameH()); // 获取hive-db的location路径
//String hive_dw_db_location = "D:/mr_file/20190211/product_storage.db/"; // 测试
		
		configuration.set(Constants.HIVE_DW_LOCATION_CONF, hive_dw_db_location); // 快照层db-location
		configuration.set(Constants.HIVE_DB_DW, dwdataBase.getEnnameH()); // 快照层db
		
		// ---------------------------------------------------------------------------------------
		
		HiveMetastore hiveODSMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(configuration.get("hdfs.load.hive.ods.path"), "db"); // 获取近源数据应用层配置文件相关信息
		List<HiveDataBase> odsdataBaseList = hiveODSMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveODSDataSet> odsdataBase = odsdataBaseList.get(0);
		String hive_ods_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, odsdataBase.getEnnameH()); // 获取hive-db的location路径
//String hive_ods_db_location = "D:/mr_file/20190211/product_storage_ods.db/"; // 测试
		
		configuration.set(Constants.HIVE_ODS_LOCATION_CONF, hive_ods_db_location); // 应用层db-location
		configuration.set(Constants.HIVE_DB_ODS, odsdataBase.getEnnameH()); // 应用层db
		
		// ---------------------------------------------------------------------------------------

		Job job = getJobInstance(configuration);

		job.setJarByClass(HdfsToHive.class);

		
		job.setMapperClass(HdfsToHiveMapper.class);

		job.setInputFormatClass(SequenceFileInputFormatOWInputSplit.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path hpo = new Path(configuration.get("hive.data.process.other", "/hive_process/other_one/")); // 这里的目录并不是hive表数据预处理输入的目录，是MultipleOutputs产生的其它文件
		if(HadoopUtil.fileExists(fileSystem, hpo)) { // 如果hive预处理输出其它文件目录已经存在则删除
			HadoopUtil.delete(fileSystem, hpo, true);
		}

		FileOutputFormat.setOutputPath(job, hpo); // 数据处理完成后其它文件输出目录
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class HdfsToHiveMapper extends Mapper<Text, Text, NullWritable, Text> {
		NullWritable ouputKey = NullWritable.get();
		Text outputValue = new Text();
		
		List<String> zkfieldList = new ArrayList<String>();
		List<String> odsfieldList = new ArrayList<String>();
		
		String zkoutputPath; // 快照层输出路径
		String odsoutputPath; // 应用层输出路径
		
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		HiveDWDataSet hiveDWDataSet; // 当前数据切片导入hive快照层配置项
		HiveODSDataSet hiveODSDataSet; // 当前数据切片导入hive应用层配置项

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			Configuration configuration = context.getConfiguration();

			String dwhiveDB = configuration.get(Constants.HIVE_DB_DW);
			String odshiveDB = configuration.get(Constants.HIVE_DB_ODS);

			String dw_db_location = configuration.get(Constants.HIVE_DW_LOCATION_CONF);	// 快照层location
			String ods_db_location = configuration.get(Constants.HIVE_ODS_LOCATION_CONF);	// 应用层location
			
			OWFileSplit owSplit = (OWFileSplit) context.getInputSplit();
			
			/**
			 * 组装向hive快照层表load数据信息
			 */
			hiveDWDataSet = owSplit.getHiveDWDataSet();
			if(hiveDWDataSet != null) {
				String dwtable = hiveDWDataSet.getEnnameH(); // 快照层表名
				String zkcolumns = JedisOperation.getForMap(Constants.HIVE_TAB_SCHEAM, dwhiveDB + Constants.SPECIALCOMMA + dwtable);
				zkoutputPath = dw_db_location + "/" + dwtable + "/" + Constants.PART_LOG_DAY + DateUtils.getYesterdayDate() + "/" + dwtable;
//				zkoutputPath = dw_db_location + "/" + dwtable + "/" + Constants.PART_LOG_DAY + DateUtils.getYesterdayDate() + Constants.HIVE_DATA_PROCESS + "/" + dwtable;
				
				if(StringUtils.isNotBlank(zkcolumns)) {
					JSONArray jsonArray = JSONArray.fromObject(zkcolumns);
					Object[] fields = jsonArray.toArray();
					
					for(Object field : fields) {
						zkfieldList.add(String.valueOf(field));
					}
				}
			}
			
			/**
			 * 组装向hive应用层表load数据信息
			 */
			hiveODSDataSet = owSplit.getHiveODSDataSet();
			if(hiveODSDataSet != null) {
				String odstable = hiveODSDataSet.getEnnameH(); // 应用层表名
				String odscolumns = JedisOperation.getForMap(Constants.HIVE_TAB_SCHEAM, odshiveDB + Constants.SPECIALCOMMA + odstable);
				odsoutputPath = ods_db_location + "/" + odstable + "/" + odstable;
//				odsoutputPath = ods_db_location + "/" + odstable + Constants.HIVE_DATA_PROCESS + "/" + odstable;
				
				if(StringUtils.isNotBlank(odscolumns)) {
					JSONArray jsonArray = JSONArray.fromObject(odscolumns);
					Object[] fields = jsonArray.toArray();
					
					for(Object field : fields) {
						odsfieldList.add(String.valueOf(field));
					}
				}
			}
		}
		
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 *  处理数据
			 */
			String[] mysqldb_table = key.toString().split(Constants.COMMA);
			String mysqldb = mysqldb_table[0];
			String table = mysqldb_table[1];
			
			String[] strs = value.toString().split(Constants.KEY_VALUE_JOIN);
			String titles = strs[0];
			String datas = strs[1];
			
			String[] titleArray = titles.split(Constants.COMMA, -1);
			String[] dataArray = datas.split(Constants.U0001, -1);
			
			Map<String, String> dataMap = new HashMap<String, String>();
			for(int i = 0; i < titleArray.length; i++) {
				dataMap.put(titleArray[i], dataArray[i]);
			}
			
			/**
			 * 处理快照层数据输出
			 */
			if(StringUtils.isNotBlank(zkoutputPath) && zkfieldList.size() > 0) {
				StringBuffer zkdataBuf = new StringBuffer();
				for(String hiveField : zkfieldList) {
					zkdataBuf.append(dataMap.get(hiveField) + Constants.U0001);
				}
				outputValue.set(zkdataBuf.substring(0, zkdataBuf.length() - 1));
				multipleOutputs.write(ouputKey, outputValue, zkoutputPath);
			}
			
			/**
			 * 处理应用层数据输出
			 */
			if(StringUtils.isNotBlank(odsoutputPath) && odsfieldList.size() > 0) {
				StringBuffer odsdataBuf = new StringBuffer();
				for(String hiveField : zkfieldList) {
					odsdataBuf.append(dataMap.get(hiveField) + Constants.U0001);
				}
				outputValue.set(odsdataBuf.substring(0, odsdataBuf.length() - 1));
				multipleOutputs.write(ouputKey, outputValue, odsoutputPath);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}
}
