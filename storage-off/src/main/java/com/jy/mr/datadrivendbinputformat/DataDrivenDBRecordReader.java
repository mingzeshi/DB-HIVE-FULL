package com.jy.mr.datadrivendbinputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A RecordReader that reads records from a SQL table,
 * using data-driven WHERE clause splits.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DataDrivenDBRecordReader<T extends DBWritable> extends RecordReader<LongWritable, T> {
//public class DataDrivenDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

	private static final Log LOG = LogFactory.getLog(DataDrivenDBRecordReader.class);

	private String dbProductName;
	private ResultSet results = null;
	private Class<T> inputClass;
	private Configuration conf;
	private CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit split;
	private long pos = 0;
	private LongWritable key = null;
	private T value = null;
	private Connection connection;
	private PreparedStatement statement;
	private DBConfiguration dbConf;
	private String conditions;
	private String[] fieldNames;
	private String tableName;

	public DataDrivenDBRecordReader(CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table, String dbProductName) throws SQLException {
		this.inputClass = inputClass;
		this.split = split;
		this.conf = conf;
		this.connection = conn;
		this.dbConf = dbConfig;
		this.conditions = cond;
		this.fieldNames = fields;
		this.tableName = table;
		this.dbProductName = dbProductName;
	}
	
	protected Configuration getConfiguration() {
		return this.conf;
	}

  	@SuppressWarnings("unchecked")
	protected String getSelectQuery() {
		StringBuilder query = new StringBuilder();
		// DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit = (DataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();
		CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit = (CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();
		DBConfiguration dbConf = getDBConf();
		String[] fieldNames = getFieldNames();
		String tableName = getTableName();
		String conditions = getConditions();

		StringBuilder conditionClauses = new StringBuilder();
		conditionClauses.append("( ").append(dataSplit.getLowerClause());
		conditionClauses.append(" ) AND ( ").append(dataSplit.getUpperClause());
		conditionClauses.append(" )");

		if (dbConf.getInputQuery() == null) {
			// We need to generate the entire query.
			query.append("SELECT ");

			for (int i = 0; i < fieldNames.length; i++) {
				query.append(fieldNames[i]);
				if (i != fieldNames.length - 1) {
					query.append(", ");
				}
			}

			query.append(" FROM ").append(tableName);
			if (!dbProductName.startsWith("ORACLE")) {
				query.append(" AS ").append(tableName);
			}
			query.append(" WHERE ");
			if (StringUtils.isNotBlank(conditions)) {
				query.append("( ").append(conditions).append(" ) AND ");
			}

			query.append(conditionClauses.toString());

		} else {
			String inputQuery = dbConf.getInputQuery();
			if (inputQuery.indexOf(CustomDataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
				LOG.error("Could not find the clause substitution token " + CustomDataDrivenDBInputFormat.SUBSTITUTE_TOKEN + " in the query: [" + inputQuery + "]. Parallel splits may not work correctly.");
			}

			query.append(inputQuery.replace(CustomDataDrivenDBInputFormat.SUBSTITUTE_TOKEN, conditionClauses.toString()));
		}

		LOG.debug("Using query: " + query.toString());

		return query.toString();
	}

// ----------------------------------------------------------------------------------------------------------------

	protected ResultSet executeQuery(String query) throws SQLException {
		this.statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		return statement.executeQuery();
	}

	public void close() throws IOException {
		try {
			if (null != results) {
				results.close();
			}
			if (null != statement) {
				statement.close();
			}
			if (null != connection) {
				connection.commit();
				connection.close();
			}
		} catch (SQLException e) {
			throw new IOException(e.getMessage());
		}
	}

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// do nothing
	}

	/** {@inheritDoc} */
	public LongWritable getCurrentKey() {
		return key;
	}

	/** {@inheritDoc} */
	public T getCurrentValue() {
		return value;
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public T createValue() {
		return ReflectionUtils.newInstance(inputClass, conf);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public long getPos() throws IOException {
		return pos;
	}

	/**
	 * @deprecated Use {@link #nextKeyValue()}
	 */
	@Deprecated
	public boolean next(LongWritable key, T value) throws IOException {
		this.key = key;
		this.value = value;
		return nextKeyValue();
	}

	/** {@inheritDoc} */
	public float getProgress() throws IOException {
		return pos / (float) split.getLength();
	}

	/** {@inheritDoc} */
	public boolean nextKeyValue() throws IOException {
		try {
			if (key == null) {
				key = new LongWritable();
			}
			if (value == null) {
				value = createValue();
			}
			if (null == this.results) {
				// First time into this method, run the query.
				String sql = getSelectQuery();
				LOG.info("task execute sql : " + sql);
				this.results = executeQuery(sql);
			}
			if (!results.next())
				return false;

			// Set the key field value as the output key value
			key.set(pos + split.getStart());

			value.readFields(results);

			pos++;
		} catch (SQLException e) {
			throw new IOException(e.getMessage());
		}
		return true;
	}

	
	protected CustomDataDrivenDBInputFormat.DBInputSplit getSplit() {
		return split;
	}

	protected String[] getFieldNames() {
		return fieldNames;
	}

	protected String getTableName() {
		return tableName;
	}

	protected String getConditions() {
		return conditions;
	}

	protected DBConfiguration getDBConf() {
		return dbConf;
	}

	protected Connection getConnection() {
		return connection;
	}

	protected PreparedStatement getStatement() {
		return statement;
	}

	protected void setStatement(PreparedStatement stmt) {
		this.statement = stmt;
	}

}
