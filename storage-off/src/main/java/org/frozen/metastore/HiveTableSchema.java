package org.frozen.metastore;

import java.sql.Connection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.constant.ConfigConstants;
import org.frozen.constant.Constants;
import org.frozen.exception.RedisException;
import org.frozen.hive.HiveMetastoreUtil;
import org.frozen.util.JDBCUtil;
import org.frozen.util.JedisOperation;
import org.frozen.util.XmlUtil;
import org.frozen.vo.Tuple;

import net.sf.json.JSONArray;


public class HiveTableSchema implements Runnable {

	public HiveTableSchema(Configuration configuration, String xmlPath) {
		this.xmlPath = xmlPath;
		this.configuration = configuration;
	}

	private String xmlPath;
	private Configuration configuration;
	
	@Override
	public void run() {
		try {
			System.out.println("------START------");
			
			JedisOperation jedisOperation = null;
			try {
				jedisOperation = JedisOperation.getInstance(configuration.get(
						ConfigConstants.REDIS_HOST), configuration.getInt(ConfigConstants.REDIS_PORT, 6480), configuration.get(ConfigConstants.REDIS_PASSWORD));
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			HiveMetastore hiveMetastore = XmlUtil.parserLoadToHiveXML(xmlPath, null); // 获取配置文件数据库相关信息
			
			Tuple<String, String> metastoreURIS = new Tuple<String, String>(ConfigConstants.HIVE_METASTORE_URIS, configuration.get(ConfigConstants.HIVE_METASTORE_URIS));
			HiveMetastoreUtil hiveMetastoreUtil = HiveMetastoreUtil.getInstance(metastoreURIS);
			
			List<HiveDataBase> hiveDataBaseList = hiveMetastore.getHiveDataBaseList();
			
			if(jedisOperation == null)
				throw RedisException.JEDISOPERATION_NULL_EXCEPTION;
			
			for(HiveDataBase<HiveDataSet> dataBase : hiveDataBaseList) {
				String dbName = dataBase.getEnnameH();

				jedisOperation.putForMap(ConfigConstants.HIVE_DB_LOCATION, dbName, hiveMetastoreUtil.getDBLocation(dbName), -1);
				
				List<HiveDataSet> hiveDataSetList = dataBase.getHiveDataSetList();
				
				for(HiveDataSet hvieDataSet : hiveDataSetList) {
					jedisOperation.putForMap(ConfigConstants.HIVE_TAB_SCHEAM, dbName + Constants.SPECIALCOMMA + hvieDataSet.getEnnameH().toLowerCase(), JSONArray.fromObject(hiveMetastoreUtil.getTabColumns(dataBase.getEnnameH().toLowerCase(), hvieDataSet.getEnnameH().toLowerCase())).toString(), -1);
				}
			}
	
			System.out.println("------END------");
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
