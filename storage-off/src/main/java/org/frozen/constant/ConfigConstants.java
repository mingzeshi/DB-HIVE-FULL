package org.frozen.constant;

/**
 * mapreduce任务中configuration配置项
 */
public interface ConfigConstants {
	
	String IMPORT_DB_CONFIG_PATH = "import.db.config.path";
	
	String IMPORT_DB_BLACKLIST_PATH = "import.db.blacklist.path";
	String IMPORT_DB_BLACKLIST_FILTER = "import.db.blacklist.filter";
	
	String IMPORT_DB_DRIVER = "import_db_driver";
	String IMPORT_DB_URL = "import.db_url";
	String IMPORT_DB_USERNAME = "import.db.username";
	String IMPORT_DB_PASSWORD = "import.db.password";
	
	String LOAD_HIVE_CONFIG = "load.hive.config";
	
	String IMPORT_DB_CONNECTION_OPEN = "import.db.connection.open";
	String IS_CONDITIONS_FULL_DOSE_DAY = "is.conditions.full.dose.day";
	String CONDITIONS_FULL_DOSE_DAY = "conditions.full.dose.day";
	
	String JDBC_FETCH_SIZE = "jdbc.fetch.size";
	
	String HIVE_DATA_PROCESS_OTHER = "mr.data.process.other";
	String MR_DATA_PROCESS_PLAN = "mr.data.process.plan";
	
	String FILE_OUTPUT_COMPRESS = "file.output.compress";
	
	String JOB_NAME_UNIQUE = "job.name.unique";
	String JOB_EXECUTE_MULTIPLE_FORMAT = "job.execute.multiple.format";
	String DATA_LOCATION_DATE = "data.location.date";
	
	String CREATE_SPLITER_ISCONDITION = "create.spliter.isCondition";

	// redis中存储：hive元数据相关key
    String HIVE_DB_LOCATION = "hive_db_location";
    String HIVE_TAB_SCHEAM = "hive_tab_schema";
    String HIVE_SPLIT_COUNT = "hive_split_count";
    
    String PART = ".part"; // 分区字段
    String PART_TIMESTAMP_FORMAT = ".part.timestamp.format"; // 分区时间格式
    
    /**
     * 以下配置项，是后缀
     * 由于可以输出至多个Hive-Location，hc1,hcP1,hc2,hcP2
     * 拼接后：
     * 		hc1.hive.db.location, hc1.hive.db
     * 		hcP1.hive.db.location, hcP1.hive.db
     * 		hc2.hive.db.location, hc2.hive.db
     * 		hcP2.hive.db.location, hcP2.hive.db
     */
    String HIVE_LOCATION = ".hive.db.location"; // Hive-DB-Location路径
    String HIVE_DB = ".hive.db"; // Hive-DB
    
    // 文件输出位置-配置项后缀Hive、HDFS
    String LOCATION_HIVE = ".hive";
    String LOCATION_HDFS = ".hdfs";
    
    // Hive-JDBC
    String HIVE_DRIVER = "hive.connection.driver";
    String HIVE_URL = "hive.connection.url";
    String HIVE_USERNAME = "hive.connection.username";
    String HIVE_PASSWORK = "hive.connection.password";
    
    
    String HIVE_METASTORE_URIS = "hive.metastore.uris"; // Hive元数据服务地址
    
    String REDIS_HOST = "redis.host";
    String REDIS_PORT = "redis.port";
    String REDIS_PASSWORD = "redis.password";
    
    // ---局部表数据导入---
    String CONFIG_CONFIGFILE = "configFile";
    String CONFIG_DRIVER = "driver";
    String CONFIG_CONNECTION = "connection";
    String CONFIG_USERNAME = "username";
    String CONFIG_PASSWORD = "password";
    String CONFIG_JDBCFILE = "jdbcFile";
    String CONFIG_TABLES = "tables";
    String CONFIG_TABLESFILE = "tablesFile";
    String CONFIG_TARGETHIVE = "targetHive";
    String CONFIG_TARGETHDFS = "targetHDFS";
	String CONFIG_COMPRESS = "compress";
	String CONFIG_PART = "part";

}
