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
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import org.frozen.constant.Constants;
import org.frozen.util.XmlUtil;

import net.sf.json.JSONObject;

public class CustomDataDrivenDBInputFormat<T extends DBWritable> extends DBInputFormat<T> implements Configurable {

	private static final Log LOG = LogFactory.getLog(CustomDataDrivenDBInputFormat.class);
	public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

	public static class DataDrivenDBInputSplit extends DBInputFormat.DBInputSplit {

		private String lowerBoundClause;
		private String upperBoundClause;
		private String db;
		private String table;
		private String conditions;
		private String fields;
		private HiveDWDataSet hiveDWDataSet;
		private HiveODSDataSet hiveODSDataSet;

		public DataDrivenDBInputSplit() {
		}

		public DataDrivenDBInputSplit(final String lower, final String upper, String db, String table, String conditions, String fields, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) {
			this.lowerBoundClause = lower;
			this.upperBoundClause = upper;
			this.db = db;
			this.table = table;
			this.conditions = conditions;
			this.fields = fields;
			this.hiveDWDataSet = hiveDWDataSet;
		    this.hiveODSDataSet = hiveODSDataSet;
		}

		public long getLength() throws IOException {
			return 0; // unfortunately, we don't know this.
		}

		public void readFields(DataInput input) throws IOException {
			this.lowerBoundClause = Text.readString(input);
			this.upperBoundClause = Text.readString(input);
			this.db = Text.readString(input);
			this.table = Text.readString(input);
			this.conditions = Text.readString(input);
			this.fields = Text.readString(input);

			this.hiveDWDataSet = (HiveDWDataSet) JSONObject.toBean(JSONObject.fromObject(Text.readString(input).toString()), HiveDWDataSet.class);
		    this.hiveODSDataSet = (HiveODSDataSet) JSONObject.toBean(JSONObject.fromObject(Text.readString(input).toString()), HiveODSDataSet.class);
		}

		public void write(DataOutput output) throws IOException {
			Text.writeString(output, this.lowerBoundClause);
			Text.writeString(output, this.upperBoundClause);
			Text.writeString(output, this.db);
			Text.writeString(output, this.table);
			Text.writeString(output, this.conditions);
			Text.writeString(output, this.fields);

			Text.writeString(output, JSONObject.fromObject(hiveDWDataSet).toString());
		    Text.writeString(output, JSONObject.fromObject(hiveODSDataSet).toString());
		}

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
		
		public HiveDWDataSet getHiveDWDataSet() {
			return hiveDWDataSet;
		}

		public void setHiveDWDataSet(HiveDWDataSet hiveDWDataSet) {
			this.hiveDWDataSet = hiveDWDataSet;
		}

		public HiveODSDataSet getHiveODSDataSet() {
			return hiveODSDataSet;
		}

		public void setHiveODSDataSet(HiveODSDataSet hiveODSDataSet) {
			this.hiveODSDataSet = hiveODSDataSet;
		}
	}

	protected DBSplitter getSplitter(int sqlDataType) {
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
		
		HiveMetastore hiveDWMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(configuration.get("hdfs.load.hive.dw.path"), null); // 获取近源数据快照层配置文件相关信息
		HiveMetastore hiveODSMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(configuration.get("hdfs.load.hive.ods.path"), null); // 获取近源数据应用层配置文件相关信息
		
		/**
		 * 拿到快照层配置项
		 */
		List<HiveDWDataSet> hiveDWDataSetList = hiveDWMetastore.getHiveDataBaseList().get(0).getHiveDataSetList();
		Map<String, HiveDWDataSet> hiveDWTabMap = new HashMap<String, HiveDWDataSet>();
		for(HiveDWDataSet dwDataSet : hiveDWDataSetList) {
			hiveDWTabMap.put(dwDataSet.getEnnameM().toLowerCase(), dwDataSet);
		}

		/**
		 * 拿到应用层配置项
		 */
		List<HiveODSDataSet> hiveODSdataSetList = hiveODSMetastore.getHiveDataBaseList().get(0).getHiveDataSetList();
		Map<String, HiveODSDataSet> hiveODSTabMap = new HashMap<String, HiveODSDataSet>();
		for(HiveODSDataSet odsDataSet : hiveODSdataSetList) {
			hiveODSTabMap.put(odsDataSet.getEnnameM().toLowerCase(), odsDataSet);
		}
		
		// -----------------------------------------------

		ResultSet results = null;
		Statement statement = null;
		Connection connection = getConnection();
		
		String spliterSQL = "";
		
		try {

			ImportRDB_XMLDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get("import.db.config.path"), null); // 获取配置文件需要导入的所有表
			String db = dataSetDB.getEnname();
			List<ImportRDB_XMLDataSet> dataSetList = dataSetDB.getImportRDB_XMLDataSet();

//			Long batchCount = configuration.getLong("map.task.batch.count", 3000000); // maptask处理数据量

			statement = connection.createStatement();

			List<InputSplit> splits = new ArrayList<InputSplit>();
			
			boolean isOpenConnection = configuration.getBoolean("import.db.connection.open", true); // 是否启用查询条件

			boolean isFullDoseDay = configuration.getBoolean("is.conditions.full.dose.day", true); // 是否启用每月某日拉取全量数据
			String fullDoseDay = configuration.get("conditions.full.dose.day", "07"); // 每月某日拉取全量数据

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
				
				HiveDWDataSet hiveDWDataSet = null;
				HiveODSDataSet hiveODSDataSet = null;				
				
				String tableName_low = tableName.toLowerCase();
				if(hiveDWTabMap.containsKey(tableName_low)) {
					hiveDWDataSet = hiveDWTabMap.get(tableName_low);
				}
				if(hiveODSTabMap.containsKey(tableName_low)) {
					hiveODSDataSet = hiveODSTabMap.get(tableName_low);
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
				DBSplitter splitter = getSplitter(sqlDataType);
				if (null == splitter) {
					throw new IOException("Unknown SQL data type: " + sqlDataType);
				}

				configuration.set(Constants.SPLITTERDB, db); // 数据切片查询db
				configuration.set(Constants.SPLITTERTABLE, tableName); // 数据切片查询table 
				configuration.set(Constants.SPLITTERCONDITIONS, conditions); // 数据切片查询条件
				configuration.set(Constants.SPLITTERFIELDS, fields); // 数据切片查询字段，默认 *

				splits.addAll(splitter.split(configuration, results, splitCol, hiveDWDataSet, hiveODSDataSet));
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

		DataDrivenDBInputSplit dataDrivenDBInputSplit = (DataDrivenDBInputSplit) split;

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
		job.setInputFormatClass(CustomDataDrivenDBInputFormat.class);
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
