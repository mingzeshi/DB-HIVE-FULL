package org.frozen.constant;

/**
 * mapreduce任务中configuration配置项
 */
public interface ConfigConstants {
	
	String IMPORT_DB_CONFIG_PATH = "import.db.config.path";
	
	String IMPORT_DB_BLACKLIST_PATH = "import.db.blacklist.path";
	
	String IMPORT_DB_DRIVER = "import_db_driver";
	String IMPORT_DB_URL = "import.db_url";
	String IMPORT_DB_USERNAME = "import.db.username";
	String IMPORT_DB_PASSWORD = "import.db.password";
	
	String LOAD_HIVE_CONFIG = "load.hive.config";
	
	String IMPORT_DB_CONNECTION_OPEN = "import.db.connection.open";
	String IS_CONDITIONS_FULL_DOSE_DAY = "is.conditions.full.dose.day";
	String CONDITIONS_FULL_DOSE_DAY = "conditions.full.dose.day";
	
	String JDBC_FETCH_SIZE = "jdbc.fetch.size";

	
	// redis中存储：hive元数据相关key
    String HIVE_DB_LOCATION = "hive_db_location";
    String HIVE_TAB_SCHEAM = "hive_tab_schema";
    
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

}
