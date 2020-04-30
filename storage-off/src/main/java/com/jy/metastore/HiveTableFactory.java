package com.jy.metastore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jy.constant.Constants;
import com.jy.util.JDBCUtil;

public class HiveTableFactory {
	
	private String driver;
	private String url;
	private String username;
	private String password;
	private Connection connection;
	
//	private char fieldDelim = '\t';
//	private char recordDelim = '\n';
	private String hiveDatabaseName;
	private String partitionKey;
	private Map<String, Integer> externalColTypes;
	private Statement lastStatement;

	private static final Logger log = LoggerFactory.getLogger(HiveTableFactory.class);
	private static final Integer FETCHSIZE = 1000;

	public static HiveTableFactory getInstance(String driver, String url, String username, String password) {
		return new HiveTableFactory(driver, url, username, password);
	}
	
	private HiveTableFactory(String driver, String url, String username, String password) {
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
	}
	
	public List<String> createTableAll(String hiveDatabaseName, String partitionKey, String... tables) {
		this.hiveDatabaseName = hiveDatabaseName;
		this.partitionKey = partitionKey;
		
		List<String> createHiveTableHQL = new ArrayList<String>();
		
		try {
			for(String table : tables) {
				String inputTableName = table;
				String outputTableName = table;
				if(table.contains(Constants.SPECIALCHAR)) {
					table.split(Constants.COMMA);
				}
				String sql = this.getCreateTableStmt(inputTableName, outputTableName);
				createHiveTableHQL.add(sql);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return createHiveTableHQL;
	}

	private String getCreateTableStmt(String inputTableName, String outputTableName) throws Exception {
		Map<String, Integer> columnTypes;

		if (null != inputTableName) {
			columnTypes = getColumnTypes(inputTableName); // 获取业务表所有列
			externalColTypes = columnTypes;
		} else {
			throw new Exception("表名NULL");
		}

		String[] colNames = getColumnNames(inputTableName);

		StringBuilder buf = new StringBuilder();

		buf.append("CREATE TABLE IF NOT EXISTS `");
		buf.append(hiveDatabaseName).append("`.`");
		buf.append(outputTableName).append("` ( ");

		boolean first = true;
		for (String col : colNames) {
			if (col.equals(partitionKey)) {
				throw new IllegalArgumentException("Partition key " + col + " cannot " + "be a column to import.");
			}

			if (!first) {
				buf.append(", ");
			}

			first = false;

			Integer colType = columnTypes.get(col);
			String hiveColType = toHiveType(colType); // 列hive类型

			if (null == hiveColType) {
				throw new IOException("Hive does not support the SQL type for column " + col);
			}

			buf.append('`').append(col).append("` ").append(hiveColType);

			if (isHiveTypeImprovised(colType)) {
				log.warn("Column " + col + " had to be cast to a less precise type in Hive");
			}
		}

		buf.append(") ");

		if (partitionKey != null) {
			buf.append("PARTITIONED BY (").append(partitionKey).append(" STRING) ");
		}

//		buf.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
//		buf.append(getHiveOctalCharCode((int) fieldDelim));
//		buf.append("' LINES TERMINATED BY '");
//		buf.append(getHiveOctalCharCode((int) recordDelim));
		buf.append(";");

		log.debug("Create statement: " + buf.toString());
		return buf.toString();
	}

	private String[] getColumnNames(String inputTableName) throws SQLException {
		ArrayList<String> keyList = new ArrayList<String>();

		String sql = getColNamesQuery(inputTableName);
		ResultSet resultSet = execute(sql);
		
		int cols = resultSet.getMetaData().getColumnCount();
		ResultSetMetaData metadata = resultSet.getMetaData();
		
		for (int i = 1; i < cols + 1; i++) {
			String colName = metadata.getColumnLabel(i);
			if (colName == null || colName.equals("")) {
				colName = metadata.getColumnName(i);
			}

			keyList.add(colName);
		}

//		String sql = "desc " + inputTableName;		
//		while (resultSet.next()) {
//			keyList.add(resultSet.getString("Field"));
//		}
		
		return keyList.toArray(new String[keyList.size()]);
	}

	private static String toHiveType(int sqlType) {

		switch (sqlType) {
		case Types.INTEGER:
		case Types.SMALLINT:
			return "INT";
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.LONGNVARCHAR:
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
		case Types.CLOB:
			return "STRING";
		case Types.NUMERIC:
		case Types.DECIMAL:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.REAL:
			return "DOUBLE";
		case Types.BIT:
		case Types.BOOLEAN:
			return "BOOLEAN";
		case Types.TINYINT:
			return "TINYINT";
		case Types.BIGINT:
			return "BIGINT";
		default:
			// (aaron): Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
			// BLOB, ARRAY, STRUCT, REF, JAVA_OBJECT.
			return "STRING";
		}
	}
	
	private static boolean isHiveTypeImprovised(int sqlType) {
		return sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP || sqlType == Types.DECIMAL || sqlType == Types.NUMERIC;
	}

	private static String getHiveOctalCharCode(int charNum) {
		if (charNum > 0177) {
			throw new IllegalArgumentException("Character " + charNum + " is an out-of-range delimiter");
		}

		return String.format("\\%03o", charNum);
	}

	/**
	 * @描述：根据表名获取所有列类型
	 * @param tableName
	 * @return
	 */
	private Map<String, Integer> getColumnTypes(String tableName) {
		String stmt = getColNamesQuery(tableName);
		return getColumnTypesForRawQuery(stmt);
	}

	private String getColNamesQuery(String tableName) {
		return "SELECT t.* FROM " + tableName + " AS t WHERE 1=0";
	}

	private Map<String, Integer> getColumnTypesForRawQuery(String stmt) {
		Map<String, List<Integer>> colInfo = getColumnInfoForRawQuery(stmt);
		if (colInfo == null) {
			return null;
		}
		Map<String, Integer> colTypes = new HashMap<String, Integer>();
		for (String s : colInfo.keySet()) {
			List<Integer> info = colInfo.get(s);
			colTypes.put(s, info.get(0));
		}
		return colTypes;
	}

	private Map<String, List<Integer>> getColumnInfoForRawQuery(String stmt) {
		ResultSet results;
		log.debug("Execute getColumnInfoRawQuery : " + stmt);
		try {
			results = execute(stmt);
		} catch (SQLException sqlE) {
			log.error("Error executing statement: " + sqlE.toString(), sqlE);
			release();
			return null;
		}

		try {
			Map<String, List<Integer>> colInfo = new HashMap<String, List<Integer>>();

			int cols = results.getMetaData().getColumnCount();
			ResultSetMetaData metadata = results.getMetaData();
			for (int i = 1; i < cols + 1; i++) {
				int typeId = metadata.getColumnType(i);
				int precision = metadata.getPrecision(i);
				int scale = metadata.getScale(i);

				// If we have an unsigned int we need to make extra room by
				// plopping it into a bigint
				if (typeId == Types.INTEGER && !metadata.isSigned(i)) {
					typeId = Types.BIGINT;
				}

				String colName = metadata.getColumnLabel(i);
				if (colName == null || colName.equals("")) {
					colName = metadata.getColumnName(i);
				}
				List<Integer> info = new ArrayList<Integer>(3);
				info.add(Integer.valueOf(typeId));
				info.add(precision);
				info.add(scale);
				colInfo.put(colName, info);
				log.debug("Found column " + colName + " of type " + info);
			}

			return colInfo;
		} catch (SQLException sqlException) {
			log.error("Error reading from database: " + sqlException.toString(), sqlException);
			return null;
		} finally {
			try {
				results.close();
//				getConnection().commit();
			} catch (SQLException sqlE) {
				log.error("SQLException closing ResultSet: " + sqlE.toString(), sqlE);
			}

			release();
		}
	}

	private ResultSet execute(String stmt, Object... args) throws SQLException {
		return execute(stmt, this.FETCHSIZE, args);
	}
	
	private ResultSet execute(String stmt, Integer fetchSize, Object... args) throws SQLException {
	    release();
	
	    PreparedStatement statement = null;
	    statement = this.getConnection().prepareStatement(stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	    this.getConnection().prepareStatement(stmt);
	    if (fetchSize != null) {
	      log.debug("Using fetchSize for next query: " + fetchSize);
	      statement.setFetchSize(fetchSize);
	    }
	    this.lastStatement = statement;
	    if (null != args) {
	      for (int i = 0; i < args.length; i++) {
	        statement.setObject(i + 1, args[i]);
	      }
	    }

	    log.info("Executing SQL statement: " + stmt);
	    return statement.executeQuery();
	}

	private void release() {
		if (null != this.lastStatement) {
			try {
				this.lastStatement.close();
			} catch (SQLException e) {
				log.error("Exception closing executed Statement: " + e, e);
			}
			this.lastStatement = null;
		}
	}
	
	private Connection getConnection() {
		if(this.connection == null) {
			this.connection = JDBCUtil.getConn(this.driver, this.url, this.username, this.password);
		}
		return this.connection;
	}
}
