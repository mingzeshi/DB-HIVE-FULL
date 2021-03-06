package com.jy.constant;


/**
 * 常量接口
 */

public interface Constants {

    // 分区字段
    String PART_DAY = "part_log_day";
    String PART_HOUR = "hour";
    String PART_MIN = "min";

    // 时间格式化
    String YEAR = "%tY";
    String MONTH = "%tm";
    String DAY = "%td";
    String HOUR = "%tH";
    String MIN = "%tM";

    // 分隔符号
    String TAB = "\t";
    String DATAKV = "-=";
    String U0001 = "\u0001";
    String SPOT = "\\.";
    String COMMA = "\\,";
    String COLON = "\\:";
    String LINE_N = "\n";
    String LINE_R = "\r";
    
    // 拼接符号
    String SPECIALCHAR = ",";
    String SPECIALCOMMA = ".";
    String SPECIALCOLON = ":";
    String KEY_VALUE_JOIN = "#=#=#";

    // 目录名称
    String MERGE_DATA = "UPDATE";

    // 默认时间
    String DEFAUL_TIME = "1970-01-01 08:00:00";
    
    // mr切片大小
    Long SPLIT_MAX = 268435456L;
    Long SPLIT_MIN = 134217728L;
    
    // import业务表执行引擎
    String ENGINE1 = "E1";
    String ENGINE2 = "E2";
    
    String OUT_FORMAT_SUFFIX = "_other"; // 文件其它数据输出目录后缀
    String PART_LOG_DAY = "part_log_day="; // hive表分区字段
    String HIVE_DATA_PROCESS = "_process"; // hvie数据预处理完成输出目录后缀
    
    // Bean-package路径
    String BEAN_QUALIFIELD = "com.jy.bean.";
    
    
    String SPLITTERDB = "custom.split.db"; // db配置项
    String SPLITTERTABLE = "custom.split.table"; // table配置项
    String SPLITTERFIELDS = "custom.split.fields"; // 字段配置项
    String SPLITTERCONDITIONS = "custom.split.conditions"; // 条件配置项
    String SPLITTERDATACOUNT = "custom.split.conditions"; // 条件配置项
    
    // 数据JSON key
    String COLUMNNAME = "CN";
    String COLUMNTYPE = "CT";
    String DATA = "DA";

    // 测试 redis
//    String REDIS_HOST = "10.103.27.19"; 
//    String PASSWORD = "redis123456";

    // 生产redis 相关配置
    String REDIS_HOST = "ops02.prod.data.phd2.jianlc.jlc";
    String PASSWORD = "ybeWHKRRf1U9";

    Integer REDIS_PORT = 6480; // 端口
    Integer MAX_ATTEMPTS = 10; //最多尝试次数
    Integer EXPIRE_SECONDS = 3 * 24 * 60 * 60; //过期时间

    // redis中存储：hive元数据相关key
    String HIVE_DB_LOCATION = "hive_db_location";
    String HIVE_TAB_SCHEAM = "hive_tab_schema";
    
    String HIVE_DW_LOCATION_CONF = "hive.db.dw.location"; // hive-db-location路径在configuration中存储key
    String HIVE_ODS_LOCATION_CONF = "hive.db.ods.location"; // hive-db-location路径在configuration中存储key
    String HIVE_DB_DW = "hive.db.dw"; // hive近源数据层快照层db
    String HIVE_DB_ODS = "hive.db.ods"; // hive近源数据层应用层db
}
