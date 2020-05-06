package org.frozen.mr;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.frozen.bean.importDBBean.ImportDataBean;
import org.frozen.bean.importDBBean.ImportRDBDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import org.frozen.constant.Constants;
import org.frozen.mr.HdfsToHive.HdfsToHiveMapper;
import org.frozen.mr.ImportDBTable.DBInputFormatMapper;
import org.frozen.mr.ImportDBTable.DataDrivenDBInputFormatMapper;
import org.frozen.mr.datadrivendbinputformat.CustomDataDrivenDBInputFormat;
import org.frozen.mr.dbinputformat.CustomDBInputFormat;
import org.frozen.util.DateUtils;
import org.frozen.util.HadoopTool;
import org.frozen.util.HadoopUtil;
import org.frozen.util.XmlUtil;

public class ImportDBLoadToHive_B extends HadoopTool  {

	public static void main(String[] args) throws Exception {
        execMain(new ImportDBLoadToHive_B(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf(); // 创建配置信息
		FileSystem fileSystem = FileSystem.get(configuration);
		
		// ---------------------------------------------------------------------------------------
		
		ImportRDBDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get("import.db.config.path"), "db"); // 获取配置文件数据库相关信息

		DBConfiguration.configureDB(configuration, dataSetDB.getDriver(), dataSetDB.getUrl(), dataSetDB.getUsername(), dataSetDB.getPassword()); // 通过conf创建数据库配置信息

//		FileSystem fileSystem = FileSystem.get(configuration); // 创建文件系统

		Job job = getJobInstance(configuration);

		job.setJarByClass(ImportDBLoadToHive_B.class);
		
		Configuration map1conf = new Configuration(false);

//		String engine = "E1"; // 获取参数
		String engine = null; // 获取参数
		if(Constants.ENGINE1.equals(engine)) {
			ChainMapper.addMapper(job, DBInputFormatMapper.class, LongWritable.class, ImportDataBean.class, NullWritable.class, Text.class, map1conf);
	
			job.setInputFormatClass(CustomDBInputFormat.class);
			CustomDBInputFormat.setInput(job, ImportDataBean.class);
		} else { // 默认执行引擎
			ChainMapper.addMapper(job, DataDrivenDBInputFormatMapper.class, LongWritable.class, ImportDataBean.class, Text.class, Text.class, map1conf);
	
			job.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
			CustomDataDrivenDBInputFormat.setInput(job, ImportDataBean.class);
		}
		
		job.setNumReduceTasks(0);
		

		String hdfsFileOutputPathStr = args[0] + "/" + DateUtils.getYesterdayDate() + "/" + dataSetDB.getEnname() + "/";

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		// ---------------------------------------------------------------------------------------

		HiveMetastore hiveDWMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(configuration.get("hdfs.load.hive.dw.path"), "db"); // 获取近源数据快照层配置文件相关信息
		List<HiveDataBase> dwdataBaseList = hiveDWMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveDWDataSet> dwdataBase = dwdataBaseList.get(0); // 获取hive-db的location 
//		String hive_dw_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, dwdataBase.getEnnameH()); // 获取hive-db的location路径
String hive_dw_db_location = "D:/mr_file/20190211/product_storage.db/"; // 测试
		
		configuration.set(Constants.HIVE_DW_LOCATION_CONF, hive_dw_db_location); // 快照层db-location
		configuration.set(Constants.HIVE_DB_DW, dwdataBase.getEnnameH()); // 快照层db
		
		// ---------------------------------------------------------------------------------------
		
		HiveMetastore hiveODSMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(configuration.get("hdfs.load.hive.ods.path"), "db"); // 获取近源数据应用层配置文件相关信息
		List<HiveDataBase> odsdataBaseList = hiveODSMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveODSDataSet> odsdataBase = odsdataBaseList.get(0);
//		String hive_ods_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, odsdataBase.getEnnameH()); // 获取hive-db的location路径
String hive_ods_db_location = "D:/mr_file/20190211/product_storage_ods.db/"; // 测试
		
		configuration.set(Constants.HIVE_ODS_LOCATION_CONF, hive_ods_db_location); // 应用层db-location
		configuration.set(Constants.HIVE_DB_ODS, odsdataBase.getEnnameH()); // 应用层db
		
		// ---------------------------------------------------------------------------------------
		
		ChainMapper.addMapper(job, HdfsToHiveMapper.class, Text.class, Text.class, NullWritable.class, Text.class, map1conf);
		
		
		Path hpo = new Path(configuration.get("hive.data.process.other", "/hive_process/other_one/")); // 这里的目录并不是hive表数据预处理输入的目录，是MultipleOutputs产生的其它文件
		if(HadoopUtil.fileExists(fileSystem, hpo)) { // 如果hive预处理输出其它文件目录已经存在则删除
			HadoopUtil.delete(fileSystem, hpo, true);
		}
		FileOutputFormat.setOutputPath(job, hpo); // 数据处理完成后其它文件输出目录
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
