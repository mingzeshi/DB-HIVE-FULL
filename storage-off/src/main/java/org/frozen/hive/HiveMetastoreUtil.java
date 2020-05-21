package org.frozen.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.frozen.vo.Tuple;

public class HiveMetastoreUtil {
	
	private static volatile HiveMetastoreUtil hiveMetastoreUtil;
	private static HiveConf hiveConf;
	private static HiveMetaStoreClient hiveMetaStoreClient;
	
	/**
	 * 获取单列对像
	 */
	public static HiveMetastoreUtil getInstance(Tuple<String, String> metastoreURIS) {
		if(hiveMetastoreUtil == null) {
			synchronized (HiveMetastoreUtil.class) {
				if(hiveMetastoreUtil == null) {
					hiveMetastoreUtil = new HiveMetastoreUtil();
					
					hiveMetastoreUtil.hiveConf.set(metastoreURIS.getKey(), metastoreURIS.getValue());
				}
			}
		}
		return hiveMetastoreUtil;
	}
	
	/**
	 * 获取HiveMetaStoreClient对象
	 */
	private HiveMetaStoreClient getHiveMetaStoreClient() throws Exception {
		if(hiveMetaStoreClient == null) {
			hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf); // 设置HiveMetaStore服务的地址
		}
		return hiveMetaStoreClient;
	}
	
	/**
	 * 获取Hive-Database的Location
	 * 
	 * @param database
	 * @return
	 * @throws Exception
	 */
	public String getDBLocation(String database) throws Exception {
		HiveMetaStoreClient hiveMetaStoreCli = this.getHiveMetaStoreClient();
		Database databaseBean = hiveMetaStoreCli.getDatabase(database);
		return databaseBean.getLocationUri();
	}
	
	/**
	 * 获取Hive-Table的Columns
	 * 
	 * @param database
	 * @param table
	 * @return
	 */
	public List<String> getTabColumns(String database, String table) throws Exception {
		HiveMetaStoreClient hiveMetaStoreCli = this.getHiveMetaStoreClient();
		Table tableBean = hiveMetaStoreCli.getTable(database, table);
		
		List<String> columns = new ArrayList<String>();
		
		StorageDescriptor sd = tableBean.getSd();
		List<FieldSchema> cols = sd.getCols();
		for(FieldSchema fieldSchema : cols) {
			columns.add(fieldSchema.getName());
		}
		
		return columns;
	}
}
