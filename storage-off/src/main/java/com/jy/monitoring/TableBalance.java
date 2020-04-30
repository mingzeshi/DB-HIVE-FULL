package com.jy.monitoring;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jy.bean.importDBBean.ImportRDBDataSet;
import com.jy.bean.importDBBean.ImportRDBDataSetDB;
import com.jy.bean.loadHiveBean.HiveDataBase;
import com.jy.bean.loadHiveBean.HiveMetastore;
import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.util.JDBCUtil;
import com.jy.util.XmlUtil;
import com.jy.vo.Tuple;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

/**
 * @描述：检查 hive 表  与  关系型数据库 表 是否一致
 * @author Administrator
 *
 */
public class TableBalance {
	
	private Statement lastStatement;
	
	private final Integer FETCHSIZE = 1000;
	
	private String driver;
	private String url;
	private String username;
	private String password;
	private Connection connection;
	private Map<String, Integer> externalColTypes;
	private String hiveDatabaseName;
	private String showTablesSQL;
	private String blackList;
	
	private Set<String> addColumnsDDLQueue = new HashSet<String>();;
	private Set<String> createTableDDLQueue = new HashSet<String>();
	
	private Set<String> importConfigQueue = new HashSet<String>();
	private Set<String> hiveXMLQueue = new HashSet<String>();

	private static final Logger log = LoggerFactory.getLogger(TableBalance.class);

	private static final Logger check_DB_TAB = LoggerFactory.getLogger("check_DB_TAB");
	private static final Logger checkN_TAB_COL = LoggerFactory.getLogger("checkN_TAB_COL");

	private void setDB(String driver, String url, String username, String password) {
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	private void setShowTables(String showTablesSQL) {
		this.showTablesSQL = showTablesSQL;		
	}

	public static void main(String[] args) {

		if(args.length > 0) { // 可以只检查ods层表或storage层表
			System.out.println("------------------------------------------------------");
			System.out.println("		-DimpConfP=xxx : 任务接入mysql表xml配置文件");
			System.out.println("		-DodsConfP=xxx : ods任务xml配置文件路径");
			System.out.println("		-DstoConfP=xxx : storage任务xml配置文件路径");
			System.out.println("		-DblaListP=xxx : 黑名单表配置文件");
			System.out.println("------------------------------------------------------");
			return;
		}
		
//		String importConfigPath = "C:\\Users\\Administrator\\Desktop\\文件\\20191121\\ImportConfig.xml";
//		String loadToODSPath = "C:\\Users\\Administrator\\Desktop\\文件\\20191121\\LoadToHiveODS.xml";
//		String loadToStoragePath = "C:\\Users\\Administrator\\Desktop\\文件\\20191121\\LoadToHiveStorage.xml";
//		String blackListPath = "C:\\Users\\Administrator\\Desktop\\文件\\20191121\\blacklist.txt";

		String importConfigPath = System.getProperty("impConfP"); // ImportConfig.xml 路径
		String loadToODSPath = System.getProperty("odsConfP"); // LoadToHiveODS.xml
		String loadToStoragePath = System.getProperty("stoConfP"); // LoadToHiveStorage.xml
		String blackListPath = System.getProperty("blaListP"); // 不参与检查数据库表 文件路径 
		
		String timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date());

		if(StringUtils.isNotBlank(importConfigPath) && StringUtils.isBlank(loadToODSPath) && StringUtils.isBlank(loadToStoragePath)) {
			
			log.info("---------------------------check database connection and tables exists ===START===" + timeFormat + "---------------------------");
			
			try {
				ImportRDBDataSetDB importRDBDataSetDB = XmlUtil.parserXml(importConfigPath, "tables");
				
				String jdbcUrl = importRDBDataSetDB.getUrl();
				
				String database = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
	
				TableBalance tableBalance = new TableBalance();

				check_DB_TAB.info("------------DB:" + database + "===START===" + timeFormat + "------------");
				
				tableBalance.setDB(importRDBDataSetDB.getDriver(), jdbcUrl, importRDBDataSetDB.getUsername(), importRDBDataSetDB.getPassword());

				if(tableBalance.getConnection() == null) {
					check_DB_TAB.info(database + " jdbc connection exception");
					check_DB_TAB.info("------------" + database + "===END===" + timeFormat + "------------");
					return;
				}
				
				
				String mysql = "show tables";
				String pgsql = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename";
				
				tableBalance.setShowTables(mysql);	// 默认mysql
				
				if(jdbcUrl.contains("postgresql")) { // pgsql
					tableBalance.setShowTables(pgsql);				
				}
				
				Set<String> tables = tableBalance.showTables(tableBalance.showTablesSQL); // db全表
				
				Set<String> hiveConfigTable = new HashSet<String>();
				for(ImportRDBDataSet importRDBDataSet : importRDBDataSetDB.getImportRDBDataSet()) {
					hiveConfigTable.add(importRDBDataSet.getEnname().replace("`", ""));
				}
				
				hiveConfigTable.removeAll(tables);
				
				if(hiveConfigTable.size() > 0) {
					for(String table : hiveConfigTable) {
						check_DB_TAB.info(database + " DB table '" + table + "' not exists");
					}
				}
				
			
				check_DB_TAB.info("------------DB:" + database + "===END===" + timeFormat + "------------");
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			log.info("---------------------------check database connection and tables exists ===END===" + timeFormat + "---------------------------");
			
		}

		if((StringUtils.isNotBlank(importConfigPath) && (StringUtils.isNotBlank(loadToODSPath) || StringUtils.isNotBlank(loadToStoragePath)))) {
			log.info("---------------------------START===" + timeFormat + "---------------------------");

			 /**
			  *	检查点：
			  *		1、检测：任务同步表与关系数据库表是否一致 (任务拉取的全量表是否有缺失) 	(黑名单除外) 
			  *		2、检测：hive表 与 关系数据库 表结构 是否一致 (字段数据、数据名称)
			  *		3、将异常表抛出 (写文件 或 第三方 库)
			  *
			  *	自动生成：
			  *		1、表异常：生成建表语句
			  *		2、列异常：生成添加列语句
			  *
			  *	自动添加到各个文件：
			  *		1、任务拉取表信息：ImportConfig.xml
			  *		2、hive表信息：
			  *			1)、ods层：LoadToHiveODS.xml
			  *			2)、dw(storage)层：LoadToHiveStorage.xml
			  *		3、刷新 storage分区表 每日分区：repartition.sh
			  *
			  *	执行：
			  *		1、执行建表、add column 的 hql
			  *		2、将hive最新的表结构刷新到redis中
			  *
			  *	测试：
			  *	
			  */

			try {
				ImportRDBDataSetDB importRDBDataSetDB = XmlUtil.parserXml(importConfigPath, "db");
				
				String jdbcUrl = importRDBDataSetDB.getUrl();
				
				String database = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
	
				TableBalance tableBalance = new TableBalance();
			
				checkN_TAB_COL.info("------------DB:" + database + "===START===" + timeFormat + "------------");
				
				if(StringUtils.isNotBlank(blackListPath)) {
					tableBalance.blackList = blackListPath;
				}
				
				tableBalance.setDB(importRDBDataSetDB.getDriver(), jdbcUrl, importRDBDataSetDB.getUsername(), importRDBDataSetDB.getPassword());
				
				String mysql = "show tables";
				String pgsql = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename";
				
				tableBalance.setShowTables(mysql);	// 默认mysql
				
				if(jdbcUrl.contains("postgresql")) { // pgsql
					tableBalance.setShowTables(pgsql);				
				}
	
				if(StringUtils.isNotBlank(loadToODSPath)) {
					tableBalance.execution(loadToODSPath, false);
				}
				
				if(StringUtils.isNotBlank(loadToStoragePath)) {
					tableBalance.execution(loadToStoragePath, true);
				}

				tableBalance.saveDataToFile();
				
				checkN_TAB_COL.info("------------DB:" + database + "===END===" + timeFormat + "------------");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			log.info("---------------------------END===" + timeFormat + "---------------------------");
		}
	} 
	
	private void saveDataToFile() {
		checkN_TAB_COL.info("");
		
		for(String str : createTableDDLQueue) {
			checkN_TAB_COL.info(str);
		}
		checkN_TAB_COL.info("");
		
		for(String str : addColumnsDDLQueue) {
			checkN_TAB_COL.info(str);
		}
		checkN_TAB_COL.info("");
		
		for(String str : importConfigQueue) {
			checkN_TAB_COL.info(str);
		}
		checkN_TAB_COL.info("");

		for(String str : hiveXMLQueue) {
			checkN_TAB_COL.info(str);
		}
		
		checkN_TAB_COL.info("");
	}

	/**
	 * @描述 执行
	 * @param xmlPath
	 * @param isPartitioned
	 * @throws Exception
	 */
	private void execution(String xmlPath, boolean isPartitioned) throws Exception {

		Map<String, Set<String>> dbTables = readDB(showTablesSQL); // mysql表对应所有列 
		
		Set<String> dbTablesBlackList = new HashSet<String>(); // db表不处理名单
		if(StringUtils.isNotBlank(blackList)) {
			BufferedReader br = null;
			try {
				String thisLine = null;
				br = new BufferedReader(new FileReader(blackList));
		         while ((thisLine = br.readLine()) != null) {
		        	 dbTablesBlackList.add(thisLine);
		         }       
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		// 检查与比较
		for(String table : dbTablesBlackList) { // 遍历不处理名单
			if(dbTables.containsKey(table)) {
				dbTables.remove(table);
			}
		}
		
		// hive 表结构
		Map<Tuple<String, String>, Set<String>> hiveTablesODS = readXML(xmlPath);
		// 检查与比较 hive ods 层的表
		for(Tuple<String, String> tableMH : hiveTablesODS.keySet()) {
			if(dbTables.containsKey(tableMH.getKey())) { // 如果 mysql 库中的表 在 hive 中已经拉取
				Set<String> dbTableFields = dbTables.remove(tableMH.getKey()); // 移除 db set 中的表
				Set<String> hiveTableFields = hiveTablesODS.get(tableMH);
				
				// 比较 columns 的异同
				dbTableFields.removeAll(hiveTableFields);
				
				if(dbTableFields.size() > 0) {
					generateAddColumns(tableMH.getKey(), hiveDatabaseName + "." + tableMH.getValue(), dbTableFields, isPartitioned);
				}
			}
		}
		
		// db 中存在的表，hive 中未拉取
		if(dbTables.size() > 0) {
			for(String table : dbTables.keySet()) {
				String createTablesDDL = getCreateTableStmt(table, table, isPartitioned);

				createTableDDLQueue.add(createTablesDDL);
				
				generateXML(table); // 生成配置文件中的 XML
			}
		} else {
			createTableDDLQueue.add(hiveDatabaseName + " database no exception");
		}
	}
	
	private void generateXML(String table) {
		
		String importConfig = "<DataSet ENName=\"" + table + "\" CHName=\"\" UniqueKey=\"id\" Storage=\"mysql\" Conditions=\"\" Fields=\"*\" Description=\"\"></DataSet>";
		
		String loadToHive = "<DataSet ENNameM=\"" + table + "\" ENNameH=\"" + table + "\" CHName=\"\" Description=\"\"></DataSet>";
		
		importConfigQueue.add(importConfig);		
		hiveXMLQueue.add(loadToHive);
		
	}
	
	/**
	 * @描述 生成 add columns ddl
	 * @param table
	 * @param columns
	 * @param isPartitioned
	 * @return
	 */
	private void generateAddColumns(String dbTable, String hvieTable, Set<String> columns, boolean isPartitioned) {
		
		Map<String, Integer> columnTypes = getColumnTypes(dbTable); // 获取业务表所有列
		
		for(String column : columns) {
			Integer colType = columnTypes.get(column);
			String hiveColType = null;
			try {
				hiveColType = toHiveType(colType); // 列hive类型
			} catch(NullPointerException e) {
				log.error(dbTable + "===" + hvieTable + " column " + column + " type " + hiveColType);
				throw e;
			}
			
			String isPart = "";
			
			if(isPartitioned) {
				isPart = "cascade";
			}
	
			String addColumnsDDL = "alter table " + hvieTable + " add columns(" + column + " " + hiveColType + ") " + isPart + ";";
			
			addColumnsDDLQueue.add(addColumnsDDL);
		}
	}
	
	/**
	 * @描述：获取关系型数据库所有表对应的列
	 * @param xmlPath
	 * @return
	 */
	private Map<String, Set<String>> readDB(String showTablesSQL) {
		try {
			Map<String, Set<String>> tableSchema = new HashMap<String, Set<String>>();
			Set<String> showTables = showTables(showTablesSQL);
			
			if(StringUtils.isNotBlank(blackList)) {
				BufferedReader br = null;
				try {
					String thisLine = null;
					br = new BufferedReader(new FileReader(blackList));
			         while ((thisLine = br.readLine()) != null) {
			        	 showTables.remove(thisLine);
			         }       
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if(br != null) {
						try {
							br.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			
			for (String tableName : showTables) {
				tableSchema.put(tableName, getCreateTableStmt(tableName));
			}
			return tableSchema;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private Set<String> getCreateTableStmt(String inputTableName) throws Exception {

		String[] colNames = getColumnNames(inputTableName);
		
		Set<String> valuesHashSet = new HashSet<String>();
		for(String colName : colNames) {
			valuesHashSet.add(colName.toLowerCase());
		}

		return valuesHashSet;
	}
	
	/**
	 * @描述：解析 xml
	 * @param xmlPath
	 * @return
	 */
	private Map<Tuple<String, String>, Set<String>> readXML(String xmlPath) {

		Map<Tuple<String, String>, Set<String>> hiveTableSchema = new HashMap<Tuple<String, String>, Set<String>>();

		HiveMetastore hiveMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(xmlPath, null); // 获取配置文件数据库相关信息

		Connection connection = JDBCUtil.getConn(hiveMetastore.getDriver(), hiveMetastore.getUrl(), hiveMetastore.getUsername(), hiveMetastore.getPassword());

		List<HiveDataBase> dataBaseList = hiveMetastore.getHiveDataBaseList();
		
		for (HiveDataBase<HiveDWDataSet> dataBase : dataBaseList) {
			String dbName = dataBase.getEnnameH();
			
			this.hiveDatabaseName = dbName;

			List<HiveDWDataSet> dataSetList = dataBase.getHiveDataSetList();

			for (HiveDWDataSet dataSet : dataSetList) {

				String keyM = dataSet.getEnnameM();
				String keyH = dataSet.getEnnameH();

				List<String> vlauesArray = JDBCUtil.getHiveTabColumns(connection, dataBase.getEnnameH().toLowerCase(), dataSet.getEnnameH().toLowerCase());

				Set<String> valuesHashSet = new HashSet<String>();

				for (String field : vlauesArray) {
					valuesHashSet.add(field.toLowerCase());
				}

				hiveTableSchema.put(new Tuple<String, String>(keyM, keyH), valuesHashSet);
			}
		}

		return hiveTableSchema;
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

		return keyList.toArray(new String[keyList.size()]);
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
		return "SELECT t.* FROM `" + tableName + "` AS t WHERE 1=0";
	}
	
	private Map<String, Integer> getColumnTypesForRawQuery(String stmt) {
		Map<String, List<Integer>> colInfo = getColumnInfoForRawQuery(stmt);
		if (colInfo == null) {
			return null;
		}
		Map<String, Integer> colTypes = new HashMap<String, Integer>();
		for (String s : colInfo.keySet()) {
			List<Integer> info = colInfo.get(s);
			colTypes.put(s.toLowerCase(), info.get(0)); // mysql字段类型 map列转小写
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
	
	private ResultSet execute(String stmt, Object... args) throws SQLException {
		return execute(stmt, this.FETCHSIZE, args);
	}
	
	private Set<String> showTables(String sql) {
		Set<String> tableSet = new HashSet<String>();
		try {
			ResultSet resultSet = execute(sql, this.FETCHSIZE);
			
			while(resultSet.next()) {
				tableSet.add(resultSet.getString(1));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return tableSet;
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
	    
	    try {
	    	return statement.executeQuery();
	    } catch(MySQLSyntaxErrorException e) {
	    	log.error("exception sql : " + stmt);
	    	log.error(e.getMessage(), e);
	    	throw e;
	    }
	}
	
	private Connection getConnection() {
		if(this.connection == null) {
			this.connection = JDBCUtil.getConn(this.driver, this.url, this.username, this.password);
		}
		return this.connection;
	}
	
	/**
	 * @描述 生成 hive 建表语句
	 * @param inputTableName
	 * @param outputTableName
	 * @return
	 * @throws Exception
	 */
	private String getCreateTableStmt(String inputTableName, String outputTableName, boolean isPartitioned) throws Exception {
		String partitionKey = null;
		
		if(isPartitioned) {
			partitionKey = "part_log_day";
		}
		
		Map<String, Integer> columnTypes;

		if (null != inputTableName) {
			columnTypes = getColumnTypes(inputTableName); // 获取业务表所有列类型
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

			Integer colType = columnTypes.get(col.toLowerCase());
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
	
	/**
	 * @描述 类型映射
	 * @param sqlType
	 * @return
	 */
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
}
