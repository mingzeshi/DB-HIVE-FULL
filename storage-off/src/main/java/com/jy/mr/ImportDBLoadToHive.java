package com.jy.mr;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.jy.bean.importDBBean.ImportDataBean;
import com.jy.bean.importDBBean.ImportRDBDataSetDB;
import com.jy.bean.loadHiveBean.HiveDataBase;
import com.jy.bean.loadHiveBean.HiveMetastore;
import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import com.jy.constant.Constants;
import com.jy.mr.HdfsToHive.HdfsToHiveMapper;
import com.jy.mr.ImportDBTable.DBInputFormatMapper;
import com.jy.mr.ImportDBTable.DataDrivenDBInputFormatMapper;
import com.jy.mr.datadrivendbinputformat.CustomDataDrivenDBInputFormat;
import com.jy.mr.dbinputformat.CustomDBInputFormat;
import com.jy.mr.sequenceinputformat.SequenceFileInputFormatOWInputSplit;
import com.jy.util.DateUtils;
import com.jy.util.HadoopTool;
import com.jy.util.HadoopUtil;
import com.jy.util.JedisOperation;
import com.jy.util.XmlUtil;

public class ImportDBLoadToHive extends HadoopTool  {

	public static void main(String[] args) throws Exception {
        execMain(new ImportDBLoadToHive(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
//		JobConf conf = new JobConf(ImportDBLoadToHive.class);
		Configuration configuration = getConf(); // 创建配置信息
		FileSystem fileSystem = FileSystem.get(configuration);
		
		// ---------------------------------------------------------------------------------------
		
		ImportRDBDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get("import.db.config.path"), "db"); // 获取配置文件数据库相关信息

		DBConfiguration.configureDB(configuration, dataSetDB.getDriver(), dataSetDB.getUrl(), dataSetDB.getUsername(), dataSetDB.getPassword()); // 通过conf创建数据库配置信息

//		FileSystem fileSystem = FileSystem.get(configuration); // 创建文件系统

		Job dbTohdfsJob = getJobInstance(configuration);

		dbTohdfsJob.setJarByClass(ImportDBLoadToHive.class);

//		String engine = "E1"; // 获取参数
		String engine = null; // 获取参数
		if(Constants.ENGINE1.equals(engine)) {
			dbTohdfsJob.setMapperClass(DBInputFormatMapper.class);
	
			dbTohdfsJob.setInputFormatClass(CustomDBInputFormat.class);
			CustomDBInputFormat.setInput(dbTohdfsJob, ImportDataBean.class);		
		} else { // 默认执行引擎
			dbTohdfsJob.setMapperClass(DataDrivenDBInputFormatMapper.class);

			dbTohdfsJob.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
			CustomDataDrivenDBInputFormat.setInput(dbTohdfsJob, ImportDataBean.class);
		}
		
		dbTohdfsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(dbTohdfsJob, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(dbTohdfsJob, DefaultCodec.class);
		
		dbTohdfsJob.setNumReduceTasks(0);
		
		dbTohdfsJob.setOutputKeyClass(Text.class);
		dbTohdfsJob.setOutputValueClass(Text.class);
		
		ControlledJob controlledJob1 = new ControlledJob(configuration);
		controlledJob1.setJob(dbTohdfsJob);
		
		String hdfsFileOutputPathStr = args[0] + "/" + DateUtils.getYesterdayDate() + "/" + dataSetDB.getEnname() + "/";
		FileOutputFormat.setOutputPath(dbTohdfsJob, new Path(hdfsFileOutputPathStr));
		
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		// ---------------------------------------------------------------------------------------

		HiveMetastore hiveDWMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(configuration.get("hdfs.load.hive.dw.path"), "db"); // 获取近源数据快照层配置文件相关信息
		List<HiveDataBase> dwdataBaseList = hiveDWMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveDWDataSet> dwdataBase = dwdataBaseList.get(0); // 获取hive-db的location 
		String hive_dw_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, dwdataBase.getEnnameH()); // 获取hive-db的location路径
		
		configuration.set(Constants.HIVE_DW_LOCATION_CONF, hive_dw_db_location); // 快照层db-location
		configuration.set(Constants.HIVE_DB_DW, dwdataBase.getEnnameH()); // 快照层db
		
		// ---------------------------------------------------------------------------------------
		
		HiveMetastore hiveODSMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(configuration.get("hdfs.load.hive.ods.path"), "db"); // 获取近源数据应用层配置文件相关信息
		List<HiveDataBase> odsdataBaseList = hiveODSMetastore.getHiveDataBaseList();
		
		HiveDataBase<HiveODSDataSet> odsdataBase = odsdataBaseList.get(0);
		String hive_ods_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, odsdataBase.getEnnameH()); // 获取hive-db的location路径
		
		configuration.set(Constants.HIVE_ODS_LOCATION_CONF, hive_ods_db_location); // 应用层db-location
		configuration.set(Constants.HIVE_DB_ODS, odsdataBase.getEnnameH()); // 应用层db
		
		// ---------------------------------------------------------------------------------------

		Job hdfsToHiveJob = getJobInstance(configuration);

		hdfsToHiveJob.setJarByClass(ImportDBLoadToHive.class);

		hdfsToHiveJob.setMapperClass(HdfsToHiveMapper.class);

		hdfsToHiveJob.setInputFormatClass(SequenceFileInputFormatOWInputSplit.class);
		hdfsToHiveJob.setOutputFormatClass(TextOutputFormat.class);

		String loadToHiveInputStr = hdfsFileOutputPathStr + "*/*";
		FileInputFormat.setInputPaths(hdfsToHiveJob, new Path(loadToHiveInputStr));

		hdfsToHiveJob.setNumReduceTasks(0);
		
		hdfsToHiveJob.setOutputKeyClass(NullWritable.class);
		hdfsToHiveJob.setOutputValueClass(Text.class);
		
		ControlledJob controlledJob2 = new ControlledJob(configuration);
		controlledJob2.setJob(hdfsToHiveJob);
		
		Path hpo = new Path(configuration.get("hive.data.process.other", "/hive_process/other_one/")); // 这里的目录并不是hive表数据预处理输入的目录，是MultipleOutputs产生的其它文件
		if(HadoopUtil.fileExists(fileSystem, hpo)) { // 如果hive预处理输出其它文件目录已经存在则删除
			HadoopUtil.delete(fileSystem, hpo, true);
		}
		FileOutputFormat.setOutputPath(hdfsToHiveJob, hpo); // 数据处理完成后其它文件输出目录
		
		// ======================================================================================
		// 总控制
		controlledJob2.addDependingJob(controlledJob1);// 依赖关系
		
		JobControl jc = new JobControl(this.getClass().getSimpleName() + "_" + DateUtils.formatNumber(new Date()));
		jc.addJob(controlledJob1);
		jc.addJob(controlledJob2);

		Thread thread = new Thread(jc);
		thread.start();
		
		while(true) {
			if(jc.allFinished()) { // 如果作业成功完成，就打印成功作业信息
				System.out.println(jc.getSuccessfulJobList());
				jc.stop();
				break;
			}
		}
		return 0;
	}
}
