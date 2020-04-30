package com.jy.mr.dbinputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.jy.bean.importDBBean.ImportRDBDataSet;
import com.jy.bean.importDBBean.ImportRDBDataSetDB;
import com.jy.constant.Constants;
import com.jy.util.XmlUtil;

public class CustomDBInputFormat<T extends DBWritable> extends InputFormat<LongWritable, T> implements Configurable {

	private String dbProductName = "DEFAULT";

	public static class NullDBWritable implements DBWritable, Writable {
		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void readFields(ResultSet arg0) throws SQLException {
		}

		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void write(PreparedStatement arg0) throws SQLException {
		}
	}

	public static class DBInputSplit extends InputSplit implements Writable {

		private long end = 0;
		private long start = 0;
		private String db;
		private String table;
		private String conditions;
		private String fields;

		public DBInputSplit() {
		}

		public DBInputSplit(long start, long end, String db, String table, String conditions, String fields) {
			this.start = start;
			this.end = end;
			this.db = db;
			this.table = table;
			this.conditions = conditions;
			this.fields = fields;
		}

		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		public long getStart() {
			return start;
		}

		public long getEnd() {
			return end;
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

		public long getLength() throws IOException {
			return end - start;
		}

		public void readFields(DataInput input) throws IOException {
			start = input.readLong();
			end = input.readLong();
			db = input.readUTF();
			table = input.readUTF();
			conditions = input.readUTF();
			fields = input.readUTF();
		}

		public void write(DataOutput output) throws IOException {
			output.writeLong(start);
			output.writeLong(end);
			output.writeUTF(db);
			output.writeUTF(table);
			output.writeUTF(conditions);
			output.writeUTF(fields);
		}
	}

	private String conditions;

	private Connection connection;

	private String tableName;

	private String[] fieldNames;

	private DBConfiguration dbConf;

	// ==================================================================

	@Override
	public Configuration getConf() {
		return dbConf.getConf();
	}

	public DBConfiguration getDBConf() {
		return dbConf;
	}

	@Override
	public void setConf(Configuration conf) {

		dbConf = new DBConfiguration(conf);

		try {
			getConnection();

			DatabaseMetaData dbMeta = connection.getMetaData();
			this.dbProductName = dbMeta.getDatabaseProductName().toUpperCase();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		tableName = dbConf.getInputTableName();
		fieldNames = dbConf.getInputFieldNames();
		conditions = dbConf.getInputConditions();
	}

	public Connection getConnection() {
		try {
			if (null == this.connection) {
				this.connection = dbConf.getConnection();
				this.connection.setAutoCommit(false);
				this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return connection;
	}

	public String getDBProductName() {
		return dbProductName;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
		ResultSet results = null;
		Statement statement = null;
		try {
			Configuration configuration = job.getConfiguration();
			ImportRDBDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get("import.db.config.path"), null); // 获取配置文件需要导入的所有表
			String db = dataSetDB.getEnname();
			List<ImportRDBDataSet> dataSetList = dataSetDB.getImportRDBDataSet();
			
			Long batchCount = configuration.getLong("map.task.batch.count", 500000); // maptask处理数据量
			
			statement = connection.createStatement();

//			int chunks = configuration.getInt("mapred.map.tasks", 1);
			
			List<InputSplit> splits = new ArrayList<InputSplit>();

			// 构建整库全表split切片
			for(ImportRDBDataSet dataSet : dataSetList) { // 循环每张表
				String tableName = dataSet.getEnname();
				
				results = statement.executeQuery(getCountQuery(tableName)); // 查询表count
				results.next();

				long count = results.getLong(1);
				long chunks = 1;
				
				if(count > (batchCount * 1.2)) {
					BigDecimal bigDecimal = new BigDecimal(count).divide(new BigDecimal(batchCount));			
					String[] nums = String.valueOf(bigDecimal.doubleValue()).split(Constants.SPOT);
					Long num = Long.parseLong(nums[1]);
					if(num > 0) {
						chunks = Long.parseLong(nums[0]) + 1;
					}
				}

				for (int i = 0; i < chunks; i++) {
					if ((i + 1) == chunks) {
						splits.add(new DBInputSplit(i * batchCount, count, db, tableName, dataSet.getConditions(), dataSet.getFields()));
					} else {				
						splits.add(new DBInputSplit(i * batchCount, (i * batchCount) + batchCount, db, tableName, dataSet.getConditions(), dataSet.getFields()));
					}
				}
			}
			
			results.close();
			statement.close();

			connection.commit();
			return splits;
		} catch (SQLException e) {
			throw new IOException("Got SQLException", e);
		} finally {
			try {
				if (results != null) {
					results.close();
				}
			} catch (SQLException e1) {
			}
			try {
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e1) {
			}

			closeConnection();
		}
	}

	protected String getCountQuery(String tableName) {
		StringBuilder query = new StringBuilder();
		query.append("SELECT COUNT(*) FROM " + tableName);

//		if (conditions != null && conditions.length() > 0)
//			query.append(" WHERE " + conditions);
		return query.toString();
	}

	protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split, Configuration conf, String tabneName_b) throws IOException {

		@SuppressWarnings("unchecked")
		Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
		try {
			if (dbProductName.startsWith("MYSQL")) {
//				return new MySQLDBRecordReader<T>(split, inputClass, conf, getConnection(), getDBConf(), conditions, fieldNames, tableName);
				return new MySQLDBRecordReader<T>(split, inputClass, conf, getConnection(), getDBConf(), split.getConditions(), split.getFields().split(Constants.COMMA), tabneName_b);
			}
		} catch (SQLException ex) {
			throw new IOException(ex.getMessage());
		}
		return null;
	}

	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		DBInputSplit dbSplit = (DBInputSplit) split;
		return createDBRecordReader(dbSplit, context.getConfiguration(), dbSplit.getTable());
	}
	
	public static void setInput(Job job, Class<? extends DBWritable> inputClass) {
		job.setInputFormatClass(CustomDBInputFormat.class);
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
//		dbConf.setInputFieldNames(fieldNames);
		dbConf.setInputClass(inputClass);
	}

	/*
	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName, String conditions, String orderBy, String... fieldNames) {
		job.setInputFormatClass(ImportALLDBInputFormat.class);
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
		dbConf.setInputClass(inputClass);
		dbConf.setInputTableName(tableName);
		dbConf.setInputFieldNames(fieldNames);
		dbConf.setInputConditions(conditions);
		dbConf.setInputOrderBy(orderBy);
	}
	*/

	/*
	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String inputQuery, String inputCountQuery) {
		job.setInputFormatClass(ImportALLDBInputFormat.class);
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
		dbConf.setInputClass(inputClass);
		dbConf.setInputQuery(inputQuery);
		dbConf.setInputCountQuery(inputCountQuery);
	}
	*/

	protected void closeConnection() {
		try {
			if (null != this.connection) {
				this.connection.close();
				this.connection = null;
			}
		} catch (SQLException sqlE) {
		}
	}	
}
