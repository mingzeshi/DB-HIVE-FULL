package com.jy.metastore;

import java.sql.Connection;
import java.util.List;

import net.sf.json.JSONArray;

import com.jy.bean.loadHiveBean.HiveDataBase;
import com.jy.bean.loadHiveBean.HiveMetastore;
import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import com.jy.constant.Constants;
import com.jy.util.JDBCUtil;
import com.jy.util.JedisOperation;
import com.jy.util.XmlUtil;


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
		
		for(HiveDataBase<HiveDWDataSet> dataBase : zkdataBaseList) {
			String dbName = dataBase.getEnnameH();
			JedisOperation.putForMap(Constants.HIVE_DB_LOCATION, dbName, JDBCUtil.getHiveDBLocation(connection, dbName), -1);
			
			List<HiveDWDataSet> zkDataSetList = dataBase.getHiveDataSetList();
			
			for(HiveDWDataSet zkDataSet : zkDataSetList) {
				
				JedisOperation.putForMap(Constants.HIVE_TAB_SCHEAM, dbName + Constants.SPECIALCOMMA + zkDataSet.getEnnameH().toLowerCase(), JSONArray.fromObject(JDBCUtil.getHiveTabColumns(connection, dataBase.getEnnameH().toLowerCase(), zkDataSet.getEnnameH().toLowerCase())).toString(), -1);
			}
		}
		
		// ----------------------------------------------------------------------------------------------------
		
		HiveMetastore odshiveMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(args[1], null); // 获取配置文件数据库相关信息
		
//		Connection odsconnection = JDBCUtil.getConn(odshiveMetastore.getDriver(), odshiveMetastore.getUrl(), odshiveMetastore.getUsername(), odshiveMetastore.getPassword());
		
		List<HiveDataBase> odsdataBaseList = odshiveMetastore.getHiveDataBaseList();
		
		for(HiveDataBase<HiveODSDataSet> dataBase : odsdataBaseList) {
			String dbName = dataBase.getEnnameH();
			JedisOperation.putForMap(Constants.HIVE_DB_LOCATION, dbName, JDBCUtil.getHiveDBLocation(connection, dbName), -1);
			
			List<HiveODSDataSet> odsDataSetList = dataBase.getHiveDataSetList();
			
			for(HiveODSDataSet odsDataSet : odsDataSetList) {
				
				JedisOperation.putForMap(Constants.HIVE_TAB_SCHEAM, dbName + Constants.SPECIALCOMMA + odsDataSet.getEnnameH().toLowerCase(), JSONArray.fromObject(JDBCUtil.getHiveTabColumns(connection, dataBase.getEnnameH().toLowerCase(), odsDataSet.getEnnameH().toLowerCase())).toString(), -1);
			}
		}
		
		System.out.println("------END------");
	}

}
