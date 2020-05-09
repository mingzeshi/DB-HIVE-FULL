package org.frozen.metastore;

import java.sql.Connection;
import java.util.List;

import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.constant.Constants;
import org.frozen.util.JDBCUtil;
import org.frozen.util.JedisOperation;
import org.frozen.util.XmlUtil;

import net.sf.json.JSONArray;


public class HiveTableSchema {

	public static void main(String[] args) {
//		args = new String[] {
//				"C:/Users/Administrator/Desktop/文件/20190821/stock_market/LoadToHiveStorage.xml",
//				"C:/Users/Administrator/Desktop/文件/20190821/stock_market/LoadToHiveODS.xml"
//		};
		
		System.out.println("------START------");
		
		HiveMetastore dwhiveMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(args[0], null); // 获取配置文件数据库相关信息
		
		Connection connection = JDBCUtil.getConn(dwhiveMetastore.getDriver(), dwhiveMetastore.getUrl(), dwhiveMetastore.getUsername(), dwhiveMetastore.getPassword());
		
		List<HiveDataBase> zkdataBaseList = dwhiveMetastore.getHiveDataBaseList();
		
		for(HiveDataBase<HiveDataSet> dataBase : zkdataBaseList) {
			String dbName = dataBase.getEnnameH();
			JedisOperation.putForMap(Constants.HIVE_DB_LOCATION, dbName, JDBCUtil.getHiveDBLocation(connection, dbName), -1);
			
			List<HiveDataSet> zkDataSetList = dataBase.getHiveDataSetList();
			
			for(HiveDataSet zkDataSet : zkDataSetList) {
				
				JedisOperation.putForMap(Constants.HIVE_TAB_SCHEAM, dbName + Constants.SPECIALCOMMA + zkDataSet.getEnnameH().toLowerCase(), JSONArray.fromObject(JDBCUtil.getHiveTabColumns(connection, dataBase.getEnnameH().toLowerCase(), zkDataSet.getEnnameH().toLowerCase())).toString(), -1);
			}
		}
		
		// ----------------------------------------------------------------------------------------------------
		
		HiveMetastore odshiveMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(args[1], null); // 获取配置文件数据库相关信息
		
//		Connection odsconnection = JDBCUtil.getConn(odshiveMetastore.getDriver(), odshiveMetastore.getUrl(), odshiveMetastore.getUsername(), odshiveMetastore.getPassword());
		
		List<HiveDataBase> odsdataBaseList = odshiveMetastore.getHiveDataBaseList();
		
		for(HiveDataBase<HiveDataSet> dataBase : odsdataBaseList) {
			String dbName = dataBase.getEnnameH();
			JedisOperation.putForMap(Constants.HIVE_DB_LOCATION, dbName, JDBCUtil.getHiveDBLocation(connection, dbName), -1);
			
			List<HiveDataSet> odsDataSetList = dataBase.getHiveDataSetList();
			
			for(HiveDataSet odsDataSet : odsDataSetList) {
				
				JedisOperation.putForMap(Constants.HIVE_TAB_SCHEAM, dbName + Constants.SPECIALCOMMA + odsDataSet.getEnnameH().toLowerCase(), JSONArray.fromObject(JDBCUtil.getHiveTabColumns(connection, dataBase.getEnnameH().toLowerCase(), odsDataSet.getEnnameH().toLowerCase())).toString(), -1);
			}
		}
		
		System.out.println("------END------");
	}

}
