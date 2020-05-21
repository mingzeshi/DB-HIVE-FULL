package org.frozen.test.hive;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

public class HiveMetastoreTest {

	public static void main(String[] args) {
		HiveConf hiveConf = new HiveConf();

		try {
			
			hiveConf.set(
					"hive.metastore.uris", 
					"thrift://slavenode163.data.test.ds:9083,thrift://slavenode164.data.test.ds:9083");

			HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf); // 设置hiveMetaStore服务的地址

//	      this.hiveMetaStoreClient.setMetaConf("hive.metastore.client.capability.check","false"); // 当前版本2.3.4与集群3.0版本不兼容，加入此设置
			
			String dbName = "tmp";
			String tableName = "hahaha";
			
			Database database = hiveMetaStoreClient.getDatabase(dbName); // 由数据库的名称获取数据库的对象(一些基本信息)

//			Table table_new = new Table(
//					tableName, 
//					dbName, 
//					owner, 
//					createTime, 
//					lastAccessTime, 
//					retention, 
//					sd, 
//					partitionKeys, 
//					parameters, 
//					viewOriginalText, 
//					viewExpandedText, 
//					tableType
//					);
			
//			hiveMetaStoreClient.createTable(tbl);
			
//			List<String> tablesList = hiveMetaStoreClient.getAllTables(dbName); // 根据数据库名称获取所有的表名
//			
//			for(String table : tablesList) {
//				System.out.println(table);
//			}
			
			Table table = hiveMetaStoreClient.getTable(dbName, tableName); // 由表名和数据库名称获取table对象(能获取列、表信息)
//			table.
//			
//			List<FieldSchema> fieldSchemaList = table.getSd().getCols(); // 获取所有的列对象
			
//			hiveMetaStoreClient.close(); // 关闭当前连接
			
		} catch (MetaException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

}
