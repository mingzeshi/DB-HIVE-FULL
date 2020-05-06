package org.frozen.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.frozen.bean.importDBBean.ImportDataBean;
import org.frozen.bean.importDBBean.ImportRDBDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import org.frozen.constant.Constants;
import org.frozen.mr.datadrivendbinputformat.CustomDataDrivenDBInputFormat;
import org.frozen.mr.datadrivendbinputformat.CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit;
import org.frozen.mr.datadrivendbinputformat.DeleteExistsTextOutputFormat;
import org.frozen.util.DateUtils;
import org.frozen.util.HadoopTool;
import org.frozen.util.HadoopUtil;
import org.frozen.util.JedisOperation;
import org.frozen.util.XmlUtil;

import net.sf.json.JSONArray;

public class ImportDBToHive extends HadoopTool {
	
	public static void main(String[] args) throws Exception {
        execMain(new ImportDBToHive(), args);
    }
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf(); // 创建配置信息
		FileSystem fileSystem = FileSystem.get(configuration); // 创建文件系统

		// ---------------------------------------------------------------------------------------

		HiveMetastore hiveDWMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(configuration.get("hdfs.load.hive.dw.path"), "db"); // 获取近源数据快照层配置文件相关信息
		List<HiveDataBase> dwdataBaseList = hiveDWMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveDWDataSet> dwdataBase = dwdataBaseList.get(0); // 获取hive-db的location 
		String hive_dw_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, dwdataBase.getEnnameH()); // 获取hive-db的location路径
//String hive_dw_db_location = "D:/mr_file/20190211/product_storage.db/"; // 测试
		
		configuration.set(Constants.HIVE_DW_LOCATION_CONF, hive_dw_db_location); // 快照层db-location
		configuration.set(Constants.HIVE_DB_DW, dwdataBase.getEnnameH()); // 快照层db
		
		// ---------------------------------------------------------------------------------------
		
		HiveMetastore hiveodsMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(configuration.get("hdfs.load.hive.ods.path"), "db"); // 获取近源数据应用层配置文件相关信息
		List<HiveDataBase> odsdataBaseList = hiveodsMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveODSDataSet> odsdataBase = odsdataBaseList.get(0);
		String hive_ods_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, odsdataBase.getEnnameH()); // 获取hive-db的location路径
//String hive_ods_db_location = "D:/mr_file/20190211/product_storage_ods.db/"; // 测试
		
		configuration.set(Constants.HIVE_ODS_LOCATION_CONF, hive_ods_db_location); // 应用层db-location
		configuration.set(Constants.HIVE_DB_ODS, odsdataBase.getEnnameH()); // 应用层db
		
		// ---------------------------------------------------------------------------------------
		
		ImportRDBDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get("import.db.config.path"), "db"); // 获取配置文件数据库相关信息

		DBConfiguration.configureDB(configuration, dataSetDB.getDriver(), dataSetDB.getUrl(), dataSetDB.getUsername(), dataSetDB.getPassword()); // 通过conf创建数据库配置信息

		Job job = getJobInstance(configuration);

		job.setJarByClass(ImportDBToHive.class);
	
		job.setMapperClass(DataDrivenDBInputFormatMapper.class);

		job.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
		CustomDataDrivenDBInputFormat.setInput(job, ImportDataBean.class);

		job.setOutputFormatClass(DeleteExistsTextOutputFormat.class);

		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		Path hpo = new Path(configuration.get("hive.data.process.other", "/hive_process/other_one/")); // 这里的目录并不是hive表数据预处理输入的目录，是MultipleOutputs产生的其它文件
		if(HadoopUtil.fileExists(fileSystem, hpo)) { // 如果hive预处理输出其它文件目录已经存在则删除
			HadoopUtil.delete(fileSystem, hpo, true);
		}
		
		String outputCompress = configuration.get("file.output.compress", "bzip2");

		if("bzip2".equals(outputCompress)) {
			FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); // Bzip2			
		} else if("gzip".equals(outputCompress)) {
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // Gzip			
		}
		
		FileOutputFormat.setOutputPath(job, hpo); // 数据处理完成后其它文件输出目录
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @描述：使用DataDrivenDBInputFormat组件Import数据
	 * @author Administrator
	 *
	 */
	public static class DataDrivenDBInputFormatMapper extends Mapper<LongWritable, ImportDataBean, NullWritable, Text> {
		NullWritable outputKey = NullWritable.get();
		private Text outputValue = new Text();

		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		DataDrivenDBInputSplit dbSplit;
		String outputPath;
		String importDB;
		String importTable;
		String outputformatDir;

		// ----------------------------------------------

		List<String> dwfieldList = new ArrayList<String>();
		List<String> odsfieldList = new ArrayList<String>();

		String dwoutputPath; // 快照层输出路径
		String odsoutputPath; // 应用层输出路径

		HiveDWDataSet hiveDWDataSet; // 当前数据切片导入hive快照层配置项
		HiveODSDataSet hiveODSDataSet; // 当前数据切片导入hive应用层配置项

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();

			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			dbSplit = (DataDrivenDBInputSplit) context.getInputSplit();

			importDB = dbSplit.getDb();
			importTable = dbSplit.getTable();

			outputPath = importTable + "/" + importTable;
			
			// ------------------------------
		
			String dwhiveDB = configuration.get(Constants.HIVE_DB_DW);
			String odshiveDB = configuration.get(Constants.HIVE_DB_ODS);

			String dw_db_location = configuration.get("custom.dw.export", configuration.get(Constants.HIVE_DW_LOCATION_CONF));	// 快照层location
			String ods_db_location = configuration.get("custom.ods.export", configuration.get(Constants.HIVE_ODS_LOCATION_CONF));	// 应用层location
			
			String part_log_day = configuration.get("custom.dw.logday", DateUtils.getYesterdayDate()); // 默认前一天 yyyy-MM-dd			
			
			DataDrivenDBInputSplit dataDrivenDBInputSplit = (DataDrivenDBInputSplit) context.getInputSplit();
			
			/**
			 * 组装向hive快照层表load数据信息
			 */
			hiveDWDataSet = dataDrivenDBInputSplit.getHiveDWDataSet();
			if(hiveDWDataSet != null) {
				String dwtable = hiveDWDataSet.getEnnameH().toLowerCase(); // 快照层表名
				String zkcolumns = JedisOperation.getForMap(Constants.HIVE_TAB_SCHEAM, dwhiveDB + Constants.SPECIALCOMMA + dwtable);
				dwoutputPath = dw_db_location + "/" + dwtable + "/" + Constants.PART_LOG_DAY + part_log_day + "/" + dwtable;
				
				if(StringUtils.isNotBlank(zkcolumns)) {
					JSONArray jsonArray = JSONArray.fromObject(zkcolumns);
					Object[] fields = jsonArray.toArray();
					
					for(Object field : fields) {
						dwfieldList.add(String.valueOf(field));
					}
				}
			}
			
			/**
			 * 组装向hive应用层表load数据信息
			 */
			hiveODSDataSet = dataDrivenDBInputSplit.getHiveODSDataSet();
			if(hiveODSDataSet != null) {
				String odstable = hiveODSDataSet.getEnnameH().toLowerCase(); // 应用层表名
				String odscolumns = JedisOperation.getForMap(Constants.HIVE_TAB_SCHEAM, odshiveDB + Constants.SPECIALCOMMA + odstable);
				odsoutputPath = ods_db_location + configuration.get("export.ods.bak.dir", "") + "/" + odstable + "/" + odstable;
				
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
		protected void map(LongWritable key, ImportDataBean value, Context context) throws IOException, InterruptedException {

			value.setDb(importDB);
			value.setTable(importTable);
			
			// -------------------------------------
			
			/**
			 *  处理数据
			 */			
			String titles = value.getTitle();
			String datas = value.getData();
			
			String[] titleArray = titles.split(Constants.COMMA, -1);
			String[] dataArray = datas.split(Constants.U0001, -1);
			
			Map<String, String> dataMap = new HashMap<String, String>();
			for(int i = 0; i < titleArray.length; i++) {
				dataMap.put(titleArray[i], dataArray[i]);
			}
			
			/**
			 * 处理快照层数据输出
			 */
			if(StringUtils.isNotBlank(dwoutputPath) && dwfieldList.size() > 0) {
				StringBuffer zkdataBuf = new StringBuffer();
				for(String hiveField : dwfieldList) {
					zkdataBuf.append(dataMap.get(hiveField) + Constants.U0001);
				}
				outputValue.set(zkdataBuf.substring(0, zkdataBuf.length() - 1));
				multipleOutputs.write(outputKey, outputValue, dwoutputPath);
			}
			
			/**
			 * 处理应用层数据输出
			 */
			if(StringUtils.isNotBlank(odsoutputPath) && odsfieldList.size() > 0) {
				StringBuffer odsdataBuf = new StringBuffer();
				for(String hiveField : odsfieldList) {
					odsdataBuf.append(dataMap.get(hiveField) + Constants.U0001);
				}
				outputValue.set(odsdataBuf.substring(0, odsdataBuf.length() - 1));
				multipleOutputs.write(outputKey, outputValue, odsoutputPath);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}
}
