package org.frozen.asynch;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.constant.ConfigConstants;
import org.frozen.constant.Constants;
import org.frozen.exception.ImportDBToHiveException;
import org.frozen.exception.RedisException;
import org.frozen.util.JedisOperation;
import org.frozen.util.XmlUtil;

public class LoadLocationConfiguration implements Runnable {
	
	private Configuration configuration;
	
	public LoadLocationConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void run() {
		/**
		 * 导入数据源-数据库相关信息
		 */
		ImportRDB_XMLDataSetDB dataSetDB = XmlUtil.parserXml(configuration.get(ConfigConstants.IMPORT_DB_CONFIG_PATH), "db"); // 获取配置文件数据库相关信息
		configuration.set(ConfigConstants.IMPORT_DB_DRIVER, dataSetDB.getDriver()); // 驱动
		configuration.set(ConfigConstants.IMPORT_DB_URL, dataSetDB.getUrl()); // 连接
		configuration.set(ConfigConstants.IMPORT_DB_USERNAME, dataSetDB.getUsername()); // 用户名
		configuration.set(ConfigConstants.IMPORT_DB_PASSWORD, dataSetDB.getPassword()); // 密码
		
		JedisOperation jedisOperation = null;
		try {
			jedisOperation = JedisOperation.getInstance(configuration.get(
					ConfigConstants.REDIS_HOST), configuration.getInt(ConfigConstants.REDIS_PORT, 6480), configuration.get(ConfigConstants.REDIS_PASSWORD));
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		String tConfig = this.configuration.get(ConfigConstants.LOAD_HIVE_CONFIG); // 数据输出配置项
		
		String[] tConfigs = tConfig.split(Constants.COMMA);
		
		if(tConfigs.length <= 0) // 无数据输出配置项
			throw ImportDBToHiveException.NO_EXPORT_CONFIG_EXCEPTION;
		
		for(String cfg : tConfigs) { // 循环每个输出-配置文件
			
			String location_hive_config = configuration.get(cfg + ConfigConstants.LOCATION_HIVE);
			
			if(StringUtils.isBlank(location_hive_config))
				throw ImportDBToHiveException.NO_HIVE_TAB_CONFIG_EXCEPTION;

			/**
			 * 加载输出到Hive表-XML配置文件
			 */
			HiveMetastore hiveMetastore = XmlUtil.parserLoadToHiveXML(location_hive_config, "db");
			HiveDataBase<HiveDataSet> dataBaseList = hiveMetastore.getHiveDataBaseList().get(0); // 获取hive-db的location
			
			if(jedisOperation == null)
				throw RedisException.JEDISOPERATION_NULL_EXCEPTION;

			String hive_db_location = jedisOperation.getForMap(ConfigConstants.HIVE_DB_LOCATION, dataBaseList.getEnnameH()); // 获取HiveDB的Location路径
			
			this.configuration.set(cfg + ConfigConstants.HIVE_LOCATION, hive_db_location); // Hive-DB-Location
			this.configuration.set(cfg + ConfigConstants.HIVE_DB, dataBaseList.getEnnameH()); // Hive-DB
		}
	}
}
