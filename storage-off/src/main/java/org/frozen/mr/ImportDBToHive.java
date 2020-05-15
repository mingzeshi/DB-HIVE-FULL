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
import org.frozen.asynch.LoadLocationConfiguration;
import org.frozen.bean.importDBBean.ImportDataBean;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.constant.ConfigConstants;
import org.frozen.constant.Constants;
import org.frozen.exception.TaskRunningException;
import org.frozen.mr.datadrivendbinputformat.DataDrivenDBInputFormat_Develop;
import org.frozen.mr.datadrivendbinputformat.DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop;
import org.frozen.mr.datadrivendbinputformat.DeleteExistsTextOutputFormat;
import org.frozen.util.DateUtils;
import org.frozen.util.HadoopTool;
import org.frozen.util.HadoopUtil;
import org.frozen.util.JedisOperation;

import net.sf.json.JSONArray;

public class ImportDBToHive extends HadoopTool {
	
	public static void main(String[] args) throws Exception {
        execMain(new ImportDBToHive(), args);
    }
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration configuration = getConf(); // 创建配置信息
		
		Thread llcT = new Thread(new LoadLocationConfiguration(configuration)); // 加载XML文件、redis将hive的DB、Location信息存储到Configuration中
		llcT.start();
		
		FileSystem fileSystem = FileSystem.get(configuration); // 创建文件系统
		
		llcT.join(); // 等待加载配置文件完毕

		DBConfiguration.configureDB(configuration, 
				configuration.get(ConfigConstants.IMPORT_DB_DRIVER), 
				configuration.get(ConfigConstants.IMPORT_DB_URL), 
				configuration.get(ConfigConstants.IMPORT_DB_USERNAME), 
				configuration.get(ConfigConstants.IMPORT_DB_PASSWORD)); // 通过configuration创建数据库配置信息

		Job job = getJobInstance(configuration);

		job.setJarByClass(ImportDBToHive.class);
	
		job.setMapperClass(ImportDBToHiveMapper.class);

		/**
		 * 设置数据抽取组件-基于 org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat<T> 二次开发
		 */
		job.setInputFormatClass(DataDrivenDBInputFormat_Develop.class);
		DataDrivenDBInputFormat_Develop.setInput(job, ImportDataBean.class);

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
	public static class ImportDBToHiveMapper extends Mapper<LongWritable, ImportDataBean, NullWritable, Text> {
		NullWritable outputKey = NullWritable.get();
		private Text outputValue = new Text();

		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		DataDrivenDBInputSplit_Develop dbSplit;
		String outputPath;
		String importDB;
		String importTable;
		String outputformatDir;

		// ----------------------------------------------
		
		Map<String, HiveDataSet> hiveDataSetMap; // 每个数据切片的数据输出点(1~N)

		Map<String, String> outputLocation = new HashMap<String, String>(); // 数据输出的所有路径
		
		Map<String, List<String>> hiveTabFileds = new HashMap<String, List<String>>(); // Hive表字段与数据库表字段的对应关系(数据库表字段集合)

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();

			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context); // 多文件路径数据输出组件
			dbSplit = (DataDrivenDBInputSplit_Develop) context.getInputSplit();

			importDB = dbSplit.getDb();
			importTable = dbSplit.getTable();
			hiveDataSetMap = dbSplit.getHiveDataSetMap();

			outputPath = importTable + "/" + importTable;
			
			// -----------------------------------------------------
			
			for(String cfg : hiveDataSetMap.keySet()) {
				StringBuffer exportLocation = new StringBuffer();
				
				/**
				 * 组装数据输出到Hive或HDFS的一系列信息
				 * 	Hive-DB
				 * 	Hive或HFS路径
				 *  如果有分区表的分区字段、分区字段是否是时间、时间格式等
				 */
				
				/**
				 * 数据输出的Location
				 */
				String hiveLocation = configuration.get(cfg + ConfigConstants.HIVE_LOCATION); // Hive-DB-Location
				if(StringUtils.isNotBlank(configuration.get(cfg + ConfigConstants.LOCATION_HDFS))) { // 如果数据输出的路径是HDFS
					hiveLocation = configuration.get(cfg + ConfigConstants.LOCATION_HDFS);
				}
				exportLocation.append(hiveLocation + Constants.PATH); // 拼接数据输出-Location
				
				/**
				 * 收集数据输出到的Hive-DB
				 */
				String hiveDB = configuration.get(cfg + ConfigConstants.HIVE_DB); // Hive-DB

				/**
				 * 提取Hive表名、列名
				 */
				HiveDataSet hiveDataSet = hiveDataSetMap.get(cfg);
				if(hiveDataSet == null) {
					throw TaskRunningException.NO_HIVE_DATASET_EXCEPTION;
				}
				
				String hiveTable = hiveDataSet.getEnnameH().toLowerCase(); // 拿到Hive表名
				exportLocation.append(hiveTable + Constants.PATH); // 拼接数据输出-Table

				/**
				 * 提取Hive表列名
				 */
				String hvieTabColumns = JedisOperation.getForMap(ConfigConstants.HIVE_TAB_SCHEAM, hiveDB + Constants.SPECIALCOMMA + hiveTable);
				
				if(StringUtils.isNotBlank(hvieTabColumns)) {
					JSONArray jsonArray = JSONArray.fromObject(hvieTabColumns);
					Object[] fields = jsonArray.toArray();
					
					List<String> fieldList = new ArrayList<String>();
					hiveTabFileds.put(cfg, fieldList);
					
					for(Object field : fields) {
						fieldList.add(String.valueOf(field));
					}
				}
				
				if(cfg.contains(Constants.PART_MARK)) { // 如果输出的目标是分区表

					/**
					 * 组装分区字段
					 */
					String partCol = configuration.get(cfg + ConfigConstants.PART, Constants.PART_LOG_DAY); // 默认part_log_day={{timestamp}}
					
					if(partCol.contains(Constants.PART_LOG_DAY_SYMBOL)) { // 如果分区字段包括时间占位符；如果分区字段不是动态时间，则不需要解析时间格式
						String partValue = DateUtils.getYesterdayDateFormat(
								configuration.get(cfg + ConfigConstants.PART_TIMESTAMP_FORMAT, Constants.PART_LOG_DAY_TIMESTAMP_FORMAT)); // 分区字段的时间格式
						
						partCol.replace(Constants.PART_LOG_DAY_SYMBOL, partValue); // 将分区字段的实际值替换
					}
					exportLocation.append(partCol + Constants.PATH); // 拼接数据输出-分区字段
				}
				
				exportLocation.append(hiveTable); // 文件后缀
				
				outputLocation.put(cfg, exportLocation.toString()); // 将完整的数据输出Location加入集合
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
			 * 数据输出
			 */
			for(String cfg : outputLocation.keySet()) {
				String location = outputLocation.get(cfg); // 数据输出的Location
				List<String> fields = hiveTabFileds.get(cfg); // 数据输出至Hive表的Fields
				
				if(StringUtils.isNotBlank(location) && fields.size() > 0) {
					StringBuffer dataBuf = new StringBuffer();
					for(String field : fields) {
						dataBuf.append(dataMap.get(field) + Constants.U0001);
					}
					outputValue.set(dataBuf.substring(0, dataBuf.length() - 1));
					
					multipleOutputs.write(outputKey, outputValue, location); // 数据输出
				}
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
