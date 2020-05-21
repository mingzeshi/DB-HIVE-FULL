package org.frozen.monitoring;

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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.dom4j.Document;
import org.dom4j.Element;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSet;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.constant.ConfigConstants;
import org.frozen.constant.Constants;
import org.frozen.exception.TableBalanceException;
import org.frozen.hive.HiveMetastoreUtil;
import org.frozen.hive.HiveUtil;
import org.frozen.metastore.HiveTableSchema;
import org.frozen.util.FileUtil;
import org.frozen.util.HadoopTool;
import org.frozen.util.JDBCUtil;
import org.frozen.util.XmlUtil;
import org.frozen.vo.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

/**
 * @描述：检查 hive 表  与  关系型数据库 表 是否一致
 * @author Administrator
 *
 */
public class TableBalance extends HadoopTool {

	private static final Logger log = LoggerFactory.getLogger(TableBalance.class);

	public static void help() {
		throw TableBalanceException.ARGS_DEFICIENCY_EXCEPTION;
	}
	
	public static void main(String[] args) throws Exception {
//		if(args.length > 0) { // 可以只检查ods层表或storage层表
//			TableBalance.help();
//		}
		execMain(new TableBalance(), args);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		ExecutorService threadPool = Executors.newCachedThreadPool();

		Configuration configuration = getConf();

		/**
		 * 检查连接与表状态：
		 * 	数据库连接是否正常
		 *	配置文件中的表是否在数据库中存在
		 */
		String import_db_config_path = configuration.get(ConfigConstants.IMPORT_DB_CONFIG_PATH); // DB配置项
		
		ConnectionMonitor connectionMonitor = ConnectionMonitor.getInstance();
		if(connectionMonitor.checkConnectionTableExists(import_db_config_path) > 0) { // 如果配置文件中的配置项表在数据库中不存在，则启动线程将配置文件中的配置移除，从而确保任务能正常运行
			threadPool.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						Document xmlDocument = XmlUtil.loadXML(import_db_config_path);

						Queue<String> tableNotExistsQueue = connectionMonitor.getConfigTabNotExistsQueue();
						
						Element rootElement = xmlDocument.getRootElement();
						List<Element> elements = rootElement.elements();
						
						for(String tableNotExists : tableNotExistsQueue) { // 循环删除失去的表
							elements.remove(rootElement.element(tableNotExists)); // 删除节点
						}
						
						XmlUtil.writeXML(import_db_config_path, elements); // 将修改过后的XML文档写入文件中
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
		
		/**
		 * 在确保数据库连接正常、配置文件中的表状态正常，任务可以正常运行的同时
		 * 
		 * 检查Hive中的表是否小于数据库中的表
		 * 检查Hive表的字段是否小于数据库中的表的字段
		 */
		String[] tConfigs = configuration.get(ConfigConstants.LOAD_HIVE_CONFIG) // 数据输出配置项
				.split(Constants.COMMA);
		
		List<Tuple> oConfig = new ArrayList<Tuple>();
		for(String cfg : tConfigs) { // 循环每个输出-配置文件
			
			oConfig.add(new Tuple<String, String>(configuration.get(cfg + ConfigConstants.LOCATION_HIVE),
					configuration.get(cfg + ConfigConstants.PART) == null ? "" : configuration.get(cfg + ConfigConstants.PART).split(Constants.PART)[0]));
		}
		
		TableBalanceMonitor tableBalanceMonitor = TableBalanceMonitor.getInstance(); // 不导入Hive的数据库表
		tableBalanceMonitor.setBlackList(configuration.get(ConfigConstants.IMPORT_DB_BLACKLIST_PATH), configuration.get(ConfigConstants.IMPORT_DB_BLACKLIST_FILTER));
		
		Tuple<String, String> metastoreURIS = new Tuple<String, String>(ConfigConstants.HIVE_METASTORE_URIS, configuration.get(ConfigConstants.HIVE_METASTORE_URIS));
		tableBalanceMonitor.setMetastoreURIS(metastoreURIS);
		
		Integer noBalanceCount = tableBalanceMonitor.checkDBHiveTableBalance(import_db_config_path, oConfig);

		if(noBalanceCount > 0) { // 有表结构的变更、新增表

			threadPool.execute(new Runnable() { // 新建表-导入的数据库配置-XML变更
				@Override
				public void run() {
					try {
						Queue<ImportRDB_XMLDataSet> importConfigQueue = tableBalanceMonitor.getImportConfigQueue();
						
						Document xmlDocument = XmlUtil.loadXML(import_db_config_path);
						
						Element rootElement = xmlDocument.getRootElement();
						
						List<Element> elements = rootElement.elements();
						
						for (ImportRDB_XMLDataSet importConfig : importConfigQueue) { // 加入节点
							Element beanElement = importConfig.toXMLElement();
							
							elements.add(beanElement); // 加入
						}
						
						XmlUtil.writeXML(import_db_config_path, elements); // 将修改过后的XML文档写入文件中
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			
			
			/**
			 * 加载操作Hive的信息
			 */
			String DRIVER = configuration.get(ConfigConstants.HIVE_DRIVER);
			String URL = configuration.get(ConfigConstants.HIVE_URL);
			String USERNAME = configuration.get(ConfigConstants.HIVE_USERNAME);
			String PASSWORD = configuration.get(ConfigConstants.HIVE_PASSWORK);
			
			threadPool.execute(new Runnable() { // 新建表-导入到Hive表配置-XML变更
				@Override
				public void run() {
					try {
						Queue<HiveDataSet> hiveXMLQueue = tableBalanceMonitor.getHiveXMLQueue();

						HiveUtil hiveUtil = HiveUtil.getInstance();
						
						Connection connection = hiveUtil.createOrGetConnection(
								DRIVER, 
								URL, 
								USERNAME, 
								PASSWORD
								);
						
						Statement statement = connection.createStatement();

						Queue<String> addColumnsDDLQueue = tableBalanceMonitor.getAddColumnsDDLQueue();
						
						Queue<String> createTableDDLQueue = tableBalanceMonitor.getCreateTableDDLQueue();

						
						for(String cfg : tConfigs) { // 循环每个输出-配置文件
							String xmlPath = configuration.get(cfg + ConfigConstants.LOCATION_HIVE);
							
							/**
							 * 修改Hive表结构
							 */
							for(String createTableDDL : createTableDDLQueue) { // 建表
								statement.execute(createTableDDL);
								
								
							}
							
							for(String addColumnsDDL : addColumnsDDLQueue) { // 修改表结构
								statement.execute(addColumnsDDL);
							}

							
							/**
							 *  写XML
							 */
							Document xmlDocument = XmlUtil.loadXML(xmlPath);

							Element rootElement = xmlDocument.getRootElement();
							
							List<Element> elements = rootElement.elements();
							
							for (HiveDataSet hiveXML : hiveXMLQueue) { // 加入节点
								Element beanElement = hiveXML.toXMLElement();
								
								elements.add(beanElement); // 加入
							}
							
							XmlUtil.writeXML(xmlPath, elements); // 将修改过后的XML文档写入文件中
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			
			threadPool.execute(new Runnable() {
				
				@Override
				public void run() {
					Queue<String> blackListQueue = tableBalanceMonitor.getBlackListQueue();
					
					StringBuffer message = new StringBuffer();
					
					for(String black : blackListQueue) {
						message.append(black);
						message.append(Constants.LINE_N + Constants.LINE_R);
					}
					try {
						FileUtil.writeFile(configuration.get(ConfigConstants.IMPORT_DB_BLACKLIST_PATH), message.toString(), true);
					} catch(Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		
		/**
		 * 刷新Redis中的元数据信息
		 */
		for(String cfg : tConfigs) { // 循环每个输出-配置文件
			String xmlPath = configuration.get(cfg + ConfigConstants.LOCATION_HIVE);
			threadPool.execute(new HiveTableSchema(configuration, xmlPath)); // 将刷新Hive-Table-Schema的任务加入到线程池中
		}

		return 0;
	}
	
	/**
	 * 数据库链接、表状态检查
	 */
	public static class ConnectionMonitor {
		
		private ConnectionMonitor() {}
		
		private final Logger check_DB_TAB = LoggerFactory.getLogger("check_DB_TAB");
		
		private static ConnectionMonitor connectionMonitor;
		
		private Queue<String> configTabNotExistsQueue = new ConcurrentLinkedQueue<String>();
		public Queue<String> getConfigTabNotExistsQueue() {
			return configTabNotExistsQueue;
		}
		
		public static ConnectionMonitor getInstance() {
			if(connectionMonitor == null) {
				synchronized (ConnectionMonitor.class) {
					if(connectionMonitor == null) {
						connectionMonitor = new ConnectionMonitor();
					}
				}
			}
			return connectionMonitor;
		}

		/**
		 * 检查数据库链接是否正常
		 * 检查配置文件中的表是否存在
		 * 
		 * 目的：确保任务可以正常运行
		 */
		private Integer checkConnectionTableExists(String import_db_config_path) throws Exception {
			
			AtomicInteger lostCount = new AtomicInteger();
			
			String timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date());
				
			log.info("---------------------------check database connection and tables exists ---START---" + timeFormat + "---------------------------");

			/**
			 * 开始检查JDBC链接
			 */
			ImportRDB_XMLDataSetDB importRDBDataSetDB = XmlUtil.parserXml(import_db_config_path, "tables");
			
			String jdbcUrl = importRDBDataSetDB.getUrl();
			
			String database = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);

			check_DB_TAB.info("==========DB:" + database + "===START===" + timeFormat + "==========");
			
			String showTabSQL = "show tables";
			if(jdbcUrl.contains("postgresql")) { // pgsql
				showTabSQL = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename";				
			}
			
			DatabaseOperate databaseOperate = DatabaseOperate.getInstance(
					importRDBDataSetDB.getDriver(), 
					jdbcUrl, 
					importRDBDataSetDB.getUsername(), 
					importRDBDataSetDB.getPassword());
			
			databaseOperate.setShowTables(showTabSQL);

			if(databaseOperate.getConnection() == null) { // JDBC链接异常
				check_DB_TAB.info(database + " jdbc connection exception");
				check_DB_TAB.info("==========DB:" + database + "===END===" + timeFormat + "==========");
				
				throw TableBalanceException.JDBC_CONNECTION_EXCEPTION;
			}
			
			check_DB_TAB.info("==========DB:" + database + "===END===" + timeFormat + "==========");
			
			/**
			 * 开始检查配置文件中的表在数据库中是否存在
			 */
			check_DB_TAB.info("==========" + database + "===TAB===START===" + timeFormat + "==========");
			
			Set<String> tables = databaseOperate.showTables(); // db全表
			
			Set<String> hiveConfigTable = new HashSet<String>();
			for(ImportRDB_XMLDataSet importRDBDataSet : importRDBDataSetDB.getImportRDB_XMLDataSet()) {
				hiveConfigTable.add(importRDBDataSet.getEnname().replace("`", ""));
			}
			
			hiveConfigTable.removeAll(tables);
			
			if(hiveConfigTable.size() > 0) {
				for(String table : hiveConfigTable) {
					check_DB_TAB.info(database + " DB table '" + table + "' not exists");
					
					configTabNotExistsQueue.add(table); // 将不存在于数据库中-配置文件中的表移除
					
					lostCount.incrementAndGet();
				}
			}

			check_DB_TAB.info("==========" + database + "===TAB===END===" + timeFormat + "==========");

			log.info("---------------------------check database connection and tables exists ---END---" + timeFormat + "---------------------------");
			
			return lostCount.intValue();
		}
	}
	
	
	/**
	 * Hive表、字段与数据库中表、字段相关检查
	 */
	public static class TableBalanceMonitor {
		
		private TableBalanceMonitor() {}
		
		private static final Logger checkN_TAB_COL = LoggerFactory.getLogger("checkN_TAB_COL");
		
		private static String dbType = "mysql"; // 数据库类型，默认是mysql
		private Map<String, Integer> externalColTypes;
		private String hiveDatabaseName;
		private String blackList;
		private String blackListFilter;
		private static volatile TableBalanceMonitor tableBalanceMonitor;
		private Tuple<String, String> metastoreURIS;
		
		private Queue<String> addColumnsDDLQueue = new ConcurrentLinkedQueue<String>();
		private Queue<String> createTableDDLQueue = new ConcurrentLinkedQueue<String>();
		
		private Queue<ImportRDB_XMLDataSet> importConfigQueue = new ConcurrentLinkedQueue<ImportRDB_XMLDataSet>();
		private Queue<HiveDataSet> hiveXMLQueue = new ConcurrentLinkedQueue<HiveDataSet>();
		
		private Queue<String> blackListQueue = new ConcurrentLinkedQueue<String>();
		
		// ======================================================================
		
		public Queue<String> getAddColumnsDDLQueue() {
			return addColumnsDDLQueue;
		}

		public Queue<String> getCreateTableDDLQueue() {
			return createTableDDLQueue;
		}

		public Queue<ImportRDB_XMLDataSet> getImportConfigQueue() {
			return importConfigQueue;
		}

		public Queue<HiveDataSet> getHiveXMLQueue() {
			return hiveXMLQueue;
		}
		
		public Queue<String> getBlackListQueue() {
			return blackListQueue;
		}
		
		// ======================================================================

		/**
		 * 单例对象
		 */
		public static TableBalanceMonitor getInstance() {
			if(tableBalanceMonitor == null) {
				synchronized (TableBalanceMonitor.class) {
					if(tableBalanceMonitor == null) {
						tableBalanceMonitor = new TableBalanceMonitor();
					}
				}
			}
			return tableBalanceMonitor;
		}
		
		public void setBlackList(String blackList, String blackListFilter) {
			tableBalanceMonitor.blackList = blackList;
			tableBalanceMonitor.blackListFilter = blackListFilter;
		}
		
		public void setMetastoreURIS(Tuple<String, String> metastoreURIS) {
			this.metastoreURIS = metastoreURIS;
		}
		
		/**
		 * 检查每天是不是有新增的表、新增的字段
		 * 
		 * 目的：及时更新Hive的元数据，确保新增的数据能第一时间进入Hive
		 */
		private Integer checkDBHiveTableBalance(String import_db_config_path, List<Tuple> cConfig) throws Exception {
			
			AtomicInteger balanceCount = new AtomicInteger();
			
			String timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date());
				
			log.info("---------------------------START---" + timeFormat + "---------------------------");

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
			ImportRDB_XMLDataSetDB importRDBDataSetDB = XmlUtil.parserXml(import_db_config_path, "db");
			
			String jdbcUrl = importRDBDataSetDB.getUrl();
			
			String database = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
		
			checkN_TAB_COL.info("------------DB:" + database + "---START---" + timeFormat + "------------");
			
			String showTabSQL = "show tables";
			if(jdbcUrl.contains("postgresql")) { // pgsql				
				dbType = "pgsql";
				showTabSQL = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename";
			}

			DatabaseOperate databaseOperate = DatabaseOperate.getInstance(
					importRDBDataSetDB.getDriver(), 
					jdbcUrl, 
					importRDBDataSetDB.getUsername(), 
					importRDBDataSetDB.getPassword());
			
			databaseOperate.setShowTables(showTabSQL);

			for(Tuple tuple : cConfig) {
				String cfg = tuple.getKey().toString();
				String part = tuple.getValue() == null ? "" : tuple.getValue().toString();
				
				execution(databaseOperate, cfg, part, false);
				balanceCount.incrementAndGet();
			}
			
			new Thread(new Runnable() { // 结果保存到文件
				@Override
				public void run() {
					saveDataToFile();
				}
			}).start();
			
			checkN_TAB_COL.info("------------DB:" + database + "---END---" + timeFormat + "------------");

			log.info("---------------------------END---" + timeFormat + "---------------------------");
			
			return balanceCount.intValue();
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
			
			for(ImportRDB_XMLDataSet str : importConfigQueue) {
				checkN_TAB_COL.info(str.toString());
			}
			checkN_TAB_COL.info("");

			for(HiveDataSet str : hiveXMLQueue) {
				checkN_TAB_COL.info(str.toString());
			}
			checkN_TAB_COL.info("");
			
			for(String str : blackListQueue) {
				checkN_TAB_COL.info(str);
			}
			checkN_TAB_COL.info("");
		}
		
		
		/**
		 * 执行
		 */
		private void execution(DatabaseOperate databaseOperate, String xmlPath, String part, boolean isPartitioned) throws Exception {

			Map<String, Set<String>> dbTables = databaseOperate.readDB(blackList); // mysql表对应所有列 
			
			Set<String> dbTablesBlackList = new HashSet<String>(); // db表不处理名单
			if(StringUtils.isNotBlank(blackList)) {
				BufferedReader br = null;
				try {
					String thisLine = null;
					br = new BufferedReader(new FileReader(blackList));
			         while ((thisLine = br.readLine()) != null) {
			        	 if(StringUtils.isNotBlank(thisLine)) {
			        		 dbTablesBlackList.add(thisLine);
			        	 }
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
						generateAddColumns(databaseOperate, tableMH.getKey(), hiveDatabaseName + "." + tableMH.getValue(), dbTableFields, isPartitioned);
					}
				}
			}
			
			// db 中存在的表，hive 中未拉取
			if(dbTables.size() > 0) {
				for(String table : dbTables.keySet()) {
					/**
					 * 判断此表是否需要加入BlackList
					 */
					
					for(String blkFilter : blackListFilter.split(Constants.COMMA)) {
						if(table.contains(blkFilter)) { // 如果包含要过滤表的字符串
							blackListQueue.add(table);
							continue;
						}
					}
					
					String createTablesDDL = getCreateTableStmt(databaseOperate, table, table, isPartitioned, part);

					createTableDDLQueue.add(createTablesDDL);
					
					String uniqueKey = acquireUniqueKey(databaseOperate, table); // 获取任务的切片字段
					
					generateXML(table, uniqueKey); // 生成配置文件中的 XML
				}
			} else {
				createTableDDLQueue.add(hiveDatabaseName + " database no exception");
			}
		}
		
		/**
		 * 获取构建MR任务时的数据切片字段
		 */
		private String acquireUniqueKey(DatabaseOperate databaseOperate, String table) throws Exception {
			Map<String, Integer> columnTypes = databaseOperate.getColumnTypes(table);
			
			String getUniqueKey = "SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='" + table + "' AND constraint_name='PRIMARY'";
			
			ResultSet rs = databaseOperate.execute(getUniqueKey, null);
			String colunm = null; // UniqueKey
			while(rs.next()) { // 如果有主键
				colunm = rs.getString(1);
				
				Integer cType = columnTypes.get(colunm);
				if(cType == Types.INTEGER || cType == Types.TINYINT || cType == Types.SMALLINT || cType == Types.BIGINT) { // 如果能找到数字类型的主键
					break;
				}
			}
			
			if(StringUtils.isBlank(colunm)) { // 如果没有主键，就随便找一个字段
				for(String col : columnTypes.keySet()) {
					colunm = col;
					break;
				}
			}
			
			return colunm;
		}	

		private void generateXML(String table, String uniqueKey) {
			
//			String importConfig = "<DataSet ENName=\"" + table + "\" CHName=\"\" UniqueKey=\"" + uniqueKey + "\" Storage=\"" + dbType + "\" Conditions=\"\" Fields=\"*\" Description=\"\"></DataSet>";
			importConfigQueue.add(
					new ImportRDB_XMLDataSet(
							table, 
							"", 
							uniqueKey, 
							dbType, 
							"", 
							Constants.SELECT_ALL, 
							"")
					);
			
//			String loadToHive = "<DataSet ENNameM=\"" + table + "\" ENNameH=\"" + table + "\" CHName=\"\" Description=\"\"></DataSet>";
			hiveXMLQueue.add(
					new HiveDataSet(
							table, 
							table, 
							"", 
							Constants.IMPORT_APPEND_DEFAULT, 
							"", 
							null)
					);
			
//			importConfigQueue.add(importConfig);
//			hiveXMLQueue.add(loadToHive);	
		}
		
		/**
		 * @描述 生成 add columns ddl
		 * @param table
		 * @param columns
		 * @param isPartitioned
		 * @return
		 */
		private void generateAddColumns(DatabaseOperate databaseOperate, String dbTable, String hvieTable, Set<String> columns, boolean isPartitioned) {
			
			Map<String, Integer> columnTypes = databaseOperate.getColumnTypes(dbTable); // 获取业务表所有列
			
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
		
				String addColumnsDDL = "alter table `" + hvieTable + "` add columns(`" + column + "` " + hiveColType + ") " + isPart + ";";
				
				addColumnsDDLQueue.add(addColumnsDDL);
			}
		}
		
		
		/**
		 * @描述 生成 hive 建表语句
		 * @param inputTableName
		 * @param outputTableName
		 * @return
		 * @throws Exception
		 */
		private String getCreateTableStmt(DatabaseOperate databaseOperate, String inputTableName, String outputTableName, boolean isPartitioned, String part) throws Exception {
			String partitionKey = null;
			
			if(isPartitioned) {
				partitionKey = part;
			}
			
			Map<String, Integer> columnTypes;

			if (null != inputTableName) {
				columnTypes = databaseOperate.getColumnTypes(inputTableName); // 获取业务表所有列类型
				externalColTypes = columnTypes;
			} else {
				throw new Exception("表名NULL");
			}

			String[] colNames = databaseOperate.getColumnNames(inputTableName);

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

//			buf.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
//			buf.append(getHiveOctalCharCode((int) fieldDelim));
//			buf.append("' LINES TERMINATED BY '");
//			buf.append(getHiveOctalCharCode((int) recordDelim));
			buf.append(";");

			log.debug("Create statement: " + buf.toString());
			return buf.toString();
		}

		/**
		 * @描述 类型映射
		 * @param sqlType
		 * @return
		 */
		private String toHiveType(int sqlType) {

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
		
		private boolean isHiveTypeImprovised(int sqlType) {
			return sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP || sqlType == Types.DECIMAL || sqlType == Types.NUMERIC;
		}
		
		/**
		 * @描述：解析 xml
		 * @param xmlPath
		 * @return
		 */
		private Map<Tuple<String, String>, Set<String>> readXML(String xmlPath) throws Exception {

			Map<Tuple<String, String>, Set<String>> hiveTableSchema = new HashMap<Tuple<String, String>, Set<String>>();

			HiveMetastore hiveMetastore = XmlUtil.parserLoadToHiveXML(xmlPath, null); // 获取配置文件数据库相关信息
			HiveMetastoreUtil hiveMetastoreUtil = HiveMetastoreUtil.getInstance(metastoreURIS);

			List<HiveDataBase> dataBaseList = hiveMetastore.getHiveDataBaseList();
			
			for (HiveDataBase<HiveDataSet> dataBase : dataBaseList) {
				String dbName = dataBase.getEnnameH();
				
				hiveDatabaseName = dbName;

				List<HiveDataSet> dataSetList = dataBase.getHiveDataSetList();

				for (HiveDataSet dataSet : dataSetList) {

					String keyM = dataSet.getEnnameM();
					String keyH = dataSet.getEnnameH();

					List<String> vlauesArray = hiveMetastoreUtil.getTabColumns(dataBase.getEnnameH().toLowerCase(), dataSet.getEnnameH().toLowerCase());

					Set<String> valuesHashSet = new HashSet<String>();

					for (String field : vlauesArray) {
						valuesHashSet.add(field.toLowerCase());
					}

					hiveTableSchema.put(new Tuple<String, String>(keyM, keyH), valuesHashSet);
				}
			}

			return hiveTableSchema;
		}
	}
	
	/**
	 * 数据库相关的操作
	 */
	public static class DatabaseOperate {
		
		private DatabaseOperate() {}
		
		private static Statement lastStatement;
		
		private final static Integer FETCHSIZE = 1000;

		private String driver;
		private String url;
		private String username;
		private String password;
		private Connection connection;
		private String showTablesSQL;
		private static volatile DatabaseOperate databaseOperate;
		
		private void setDB(String driver, String url, String username, String password) {
			this.driver = driver;
			this.url = url;
			this.username = username;
			this.password = password;
		}

		private void setShowTables(String showTablesSQL) {
			this.showTablesSQL = showTablesSQL;		
		}

		/**
		 * 单例对象
		 */
		public static DatabaseOperate getInstance(String driver, String url, String username, String password) {
			if(databaseOperate == null) {
				synchronized (DatabaseOperate.class) {
					if(databaseOperate == null) {
						databaseOperate = new DatabaseOperate();

						databaseOperate.setDB(driver, url, username, password);
					}
				}
			}
			return databaseOperate;
		}
		
		/**
		 * @描述：获取关系型数据库所有表对应的列
		 * @param xmlPath
		 * @return
		 */
		private Map<String, Set<String>> readDB(String blackList) {
			try {
				Map<String, Set<String>> tableSchema = new HashMap<String, Set<String>>();
				Set<String> showTables = showTables();
				
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
//					getConnection().commit();
				} catch (SQLException sqlE) {
					log.error("SQLException closing ResultSet: " + sqlE.toString(), sqlE);
				}

				release();
			}
		}
		
		private void release() {
			if (null != lastStatement) {
				try {
					lastStatement.close();
				} catch (SQLException e) {
					log.error("Exception closing executed Statement: " + e, e);
				}
				lastStatement = null;
			}
		}
		
		private ResultSet execute(String stmt, Object... args) throws SQLException {
			return execute(stmt, FETCHSIZE, args);
		}
		
		private Set<String> showTables() {
			Set<String> tableSet = new HashSet<String>();
			try {
				ResultSet resultSet = execute(showTablesSQL, FETCHSIZE);
				
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
		    
		    statement = getConnection().prepareStatement(stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		    
		    getConnection().prepareStatement(stmt);
		    if (fetchSize != null) {
		      log.debug("Using fetchSize for next query: " + fetchSize);
		      statement.setFetchSize(fetchSize);
		    }
		    lastStatement = statement;
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
			if(connection == null) {
				connection = JDBCUtil.getConn(driver, url, username, password);
			}
			return connection;
		}
		
	}
}
