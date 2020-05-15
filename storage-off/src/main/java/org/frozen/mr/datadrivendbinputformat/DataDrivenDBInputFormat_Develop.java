package org.frozen.mr.datadrivendbinputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSet;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.constant.ConfigConstants;
import org.frozen.constant.Constants;
import org.frozen.exception.ImportDBToHiveException;
import org.frozen.util.JedisOperation;
import org.frozen.util.XmlUtil;

import net.sf.json.JSONObject;

public class DataDrivenDBInputFormat_Develop<T extends DBWritable> extends DBInputFormat<T> implements Configurable {

	private static final Log LOG = LogFactory.getLog(DataDrivenDBInputFormat_Develop.class);
	public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

	public static class DataDrivenDBInputSplit_Develop extends DBInputFormat.DBInputSplit {

		private String lowerBoundClause; // 下边界
		private String upperBoundClause; // 上边界
		private String db; // 数据表切片的DB库名
		private String table; // 数据表切片的表名
		private String conditions; // 数据表切片的where条件
		private String fields; // 数据表切片的列名
		private Map<String, HiveDataSet> hiveDataSetMap; // 数据表切片数据表与Hive表映射关系

		public DataDrivenDBInputSplit_Develop() {
		}

		public DataDrivenDBInputSplit_Develop(final String lower, final String upper, String db, String table, String conditions, String fields, Map<String, HiveDataSet> hiveDataSetMap) {
			this.lowerBoundClause = lower;
			this.upperBoundClause = upper;
			this.db = db;
			this.table = table;
			this.conditions = conditions;
			this.fields = fields;
			this.hiveDataSetMap = hiveDataSetMap;
		}

		public long getLength() throws IOException {
			return 0; // unfortunately, we don't know this.
		}

		/**
		 * 输入反序列化
		 */
		public void readFields(DataInput input) throws IOException {
			this.lowerBoundClause = Text.readString(input);
			this.upperBoundClause = Text.readString(input);
			this.db = Text.readString(input);
			this.table = Text.readString(input);
			this.conditions = Text.readString(input);
			this.fields = Text.readString(input);

			/**
			 * 反序列化 Map<String, HiveDataSet> 数据格式
			 */
			JSONObject jsonObject = JSONObject.fromObject(Text.readString(input).toString());
			Iterator<String> jsonKeys = jsonObject.keys();
			Map<String, HiveDataSet> hiveDataSetMap_ = new HashMap<String, HiveDataSet>();
			while(jsonKeys.hasNext()) {
				String jK = jsonKeys.next();
				hiveDataSetMap_.put(
						jK,
						(HiveDataSet) JSONObject.toBean(JSONObject.fromObject(jsonObject.get(jK)), HiveDataSet.class));
			}
			
			this.hiveDataSetMap = hiveDataSetMap_;
		}

		/**
		 * 输出序列化
		 */
		public void write(DataOutput output) throws IOException {
			Text.writeString(output, this.lowerBoundClause);
			Text.writeString(output, this.upperBoundClause);
			Text.writeString(output, this.db);
			Text.writeString(output, this.table);
			Text.writeString(output, this.conditions);
			Text.writeString(output, this.fields);

			Text.writeString(output, JSONObject.fromObject(hiveDataSetMap).toString());
		}

		/**
		 * getter、setter
		 */
		public String getLowerClause() {
			return lowerBoundClause;
		}

		public String getUpperClause() {
			return upperBoundClause;
		}

		public String getDb() {
			return db;
		}

		public String getTable() {
			return table;
		}

		public String getConditions() {
			return conditions;
		}

		public String getFields() {
			return fields;
		}
		
		public Map<String, HiveDataSet> getHiveDataSetMap() {
			return hiveDataSetMap;
		}

		public void setHiveDataSetMap(Map<String, HiveDataSet> hiveDataSetMap) {
			this.hiveDataSetMap = hiveDataSetMap;
		}
	}

	/**
	 * 根据字段类型匹配切片类型
	 */
	protected DBSplitter_Develop getSplitter(int sqlDataType) {
		switch (sqlDataType) {
		case Types.NUMERIC:
		case Types.DECIMAL:
			return new BigDecimalSplitter();

		case Types.BIT:
		case Types.BOOLEAN:
			return new BooleanSplitter();

		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.BIGINT:
			return new IntegerSplitter();

		case Types.REAL:
		case Types.FLOAT:
		case Types.DOUBLE:
			return new FloatSplitter();

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return new TextSplitter();

		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return new DateSplitter();

		default:
			// TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB,
			// BLOB, ARRAY
			// STRUCT, REF, DATALINK, and JAVA_OBJECT.
			return null;
		}
	}

	/** {@inheritDoc} */
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration configuration = job.getConfiguration();

//		int targetNumTasks = job.getConfiguration().getInt("mapred.map.tasks", 1);
//	    if (1 == targetNumTasks) {
//	      List<InputSplit> singletonSplit = new ArrayList<InputSplit>();
//	      singletonSplit.add(new DataDrivenDBInputSplit("1=1", "1=1"));
//	      return singletonSplit;
//	    }

		// -----------------------------------------------
		
		String tConfig = configuration.get(ConfigConstants.LOAD_HIVE_CONFIG); // 数据输出配置项
		
		String[] tConfigs = tConfig.split(Constants.COMMA);
		
		if(tConfigs.length <= 0) // 无数据输出配置项
			throw ImportDBToHiveException.NO_EXPORT_CONFIG_EXCEPTION;
		
		Map<String, Map<String, HiveDataSet>> hvieExportConfigMap = new HashMap<String, Map<String, HiveDataSet>>(); // 数据据输出所有的目录
		
		for(String cfg : tConfigs) { // 循环每个输出-配置文件
			
			String location_hive_config = configuration.get(cfg + ConfigConstants.LOCATION_HIVE);
			
			if(StringUtils.isBlank(location_hive_config))
				throw ImportDBToHiveException.NO_HIVE_TAB_CONFIG_EXCEPTION;

			/**
			 *  加载输出到Hive表-XML配置文件
			 */
			List<HiveDataSet> hiveDataSetList = XmlUtil.parserLoadToHiveXML(location_hive_config, null)
				.getHiveDataBaseList().get(0)
				.getHiveDataSetList();
			
			Map<String, HiveDataSet> hiveTabMap = new HashMap<String, HiveDataSet>(); // 数据库表与Hive表的映射
			
			for(HiveDataSet hiveDataSet : hiveDataSetList) {
				hiveTabMap.put(hiveDataSet.getEnnameM().toLowerCase(), hiveDataSet);
			}
			
			hvieExportConfigMap.put(cfg, hiveTabMap);
		}

		// -----------------------------------------------

		ResultSet results = null;
		Statement statement = null;
		Connection connection = getConnection();
		
		String spliterSQL = "";
		
		try {

			ImportRDB_XMLDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get(ConfigConstants.IMPORT_DB_CONFIG_PATH), null); // 获取配置文件需要导入的所有表
			String db = dataSetDB.getEnname(); // 导入数据所在库
			List<ImportRDB_XMLDataSet> dataSetList = dataSetDB.getImportRDB_XMLDataSet(); // 导入数据所有表信息

			statement = connection.createStatement();

			List<InputSplit> splits = new ArrayList<InputSplit>(); // split集合
			
			boolean isOpenConnection = configuration.getBoolean(ConfigConstants.IMPORT_DB_CONNECTION_OPEN, true); // 是否启用查询条件

			boolean isFullDoseDay = configuration.getBoolean(ConfigConstants.IS_CONDITIONS_FULL_DOSE_DAY, true); // 是否启用每月某日拉取全量数据
			String fullDoseDay = configuration.get(ConfigConstants.CONDITIONS_FULL_DOSE_DAY, "07"); // 每月某日拉取全量数据

			// 构建整库全表split切片
			for (ImportRDB_XMLDataSet dataSet : dataSetList) { // 循环每张表
				String tableName = dataSet.getEnname();
				String splitCol = dataSet.getUniqueKey(); // 主键
				String conditions = dataSet.getConditions();
				
				if(!isOpenConnection) {
					conditions = "";
				}
				
				if(isFullDoseDay && fullDoseDay.equals(new SimpleDateFormat("dd").format(new Date()))) { // 每月几号强制拉取全量数据
					conditions = "";
				}
				
				String fields = dataSet.getFields(); // 数据切片查询字段，默认 *
							
				String tableName_low = tableName.toLowerCase();
				
				Map<String, HiveDataSet> hiveDataSetMap = new HashMap<String, HiveDataSet>();
				
				for(String cfg : tConfigs) {
					if(hvieExportConfigMap.get(cfg).containsKey(tableName_low)) {
						hiveDataSetMap.put(cfg, hvieExportConfigMap.get(cfg).get(tableName_low));
					}
				}
				
				// ----------------------------------------------
				
				/**
				 *  根据条件构建切片也要花很多的时间，这里在构建切片时可以选择不使用查询条件
				 *  原因：
				 *  	例如3亿数据，利用查询条件过滤后的数据剩下1亿，每个批次数据1000万，构建10个切片，但在查询过滤时可能花费10分钟
				 *  	如果省去在构建切片时的条件过滤，那将会创建30个切片,虽然切片数增加了，任务数据也增加了，但在实际任务处理时不会对数据有影响，因为实际任务处理数据也会有条件过滤
				 */
				String spliterConditions = null;
				if(configuration.getBoolean("create.spliter.isCondition", true)) {
					spliterConditions = conditions;					
				}
				
				spliterSQL = getBoundingValsQuery(tableName, splitCol, spliterConditions);
				
				results = statement.executeQuery(spliterSQL);
				results.next();

				int sqlDataType = results.getMetaData().getColumnType(1);
				DBSplitter_Develop splitter = getSplitter(sqlDataType);
				if (null == splitter) {
					throw new IOException("Unknown SQL data type: " + sqlDataType);
				}

				configuration.set(Constants.SPLITTERDB, db); // 数据切片查询db
				configuration.set(Constants.SPLITTERTABLE, tableName); // 数据切片查询table 
				configuration.set(Constants.SPLITTERCONDITIONS, conditions); // 数据切片查询条件
				configuration.set(Constants.SPLITTERFIELDS, fields); // 数据切片查询字段，默认 *

				splits.addAll(splitter.split(configuration, results, splitCol, hiveDataSetMap));
			}

			return splits;
		} catch (SQLException e) {
			LOG.error("Exception SQL : " + spliterSQL);
			throw new IOException(e.getMessage());
		} finally {
			try {
				if (null != results) {
					results.close();
				}
			} catch (SQLException se) {
				LOG.debug("SQLException closing resultset: " + se.toString());
			}

			try {
				if (null != statement) {
					statement.close();
				}
			} catch (SQLException se) {
				LOG.debug("SQLException closing statement: " + se.toString());
			}

			try {
				connection.commit();
				closeConnection();
			} catch (SQLException se) {
				LOG.debug("SQLException committing split transaction: " + se.toString());
			}
		}
	}

	/**
	 * @描述：整体数据切片规划
	 * @param tableName
	 * @param splitCol
	 * @param conditions
	 * @return
	 */
	protected String getBoundingValsQuery(String tableName, String splitCol, String conditions) {
		// If the user has provided a query, use that instead.
		String userQuery = getDBConf().getInputBoundingQuery();
		if (null != userQuery) {
			return userQuery;
		}

		// Auto-generate one based on the table name we've been provided with.
		StringBuilder query = new StringBuilder();

		// String splitCol = getDBConf().getInputOrderBy();
		query.append("SELECT MIN(").append(splitCol).append("), ");
		query.append("MAX(").append(splitCol).append(") FROM ");
		query.append(tableName); // getDBConf().getInputTableName()
		// String conditions = getDBConf().getInputConditions();
		if (StringUtils.isNotBlank(conditions)) {
			query.append(" WHERE ( " + conditions + " )");
		}

		return query.toString();
	}

	public static void setBoundingQuery(Configuration conf, String query) {
		if (null != query) {
			if (query.indexOf(SUBSTITUTE_TOKEN) == -1) {
				LOG.warn("Could not find " + SUBSTITUTE_TOKEN + " token in query: " + query + "; splits may not partition data.");
			}
		}

		conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
	}

	protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split, Configuration conf) throws IOException {

		DBConfiguration dbConf = getDBConf();
		@SuppressWarnings("unchecked")
		Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
		String dbProductName = getDBProductName();

		LOG.debug("Creating db record reader for db product: " + dbProductName);

		DataDrivenDBInputSplit_Develop dataDrivenDBInputSplit = (DataDrivenDBInputSplit_Develop) split;

		try {
			if (dbProductName.startsWith("MYSQL") || dbProductName.startsWith("POSTGRESQL")) {
				return new MySQLDataDrivenDBRecordReader<T>(dataDrivenDBInputSplit, inputClass, conf, getConnection(), dbConf, dataDrivenDBInputSplit.getConditions(), dataDrivenDBInputSplit.getFields().split(Constants.COMMA), dataDrivenDBInputSplit.getTable());
			}
		} catch (SQLException ex) {
			throw new IOException(ex.getMessage());
		}
		return null;
	}

	public static void setInput(Job job, Class<? extends DBWritable> inputClass) {
//		DBInputFormat.setInput(job, inputClass, tableName, conditions, splitBy, fieldNames);
		job.setInputFormatClass(DataDrivenDBInputFormat_Develop.class);
	    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
//	    dbConf.setInputFieldNames(fieldNames);
		dbConf.setInputClass(inputClass);
	}
	
//	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName, String conditions, String splitBy, String... fieldNames) {
//		DBInputFormat.setInput(job, inputClass, tableName, conditions, splitBy, fieldNames);
//		job.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
//	}
//
//	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String inputQuery, String inputBoundingQuery) {
//		DBInputFormat.setInput(job, inputClass, inputQuery, "");
//		job.getConfiguration().set(DBConfiguration.INPUT_BOUNDING_QUERY, inputBoundingQuery);
//		job.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
//	}
}
