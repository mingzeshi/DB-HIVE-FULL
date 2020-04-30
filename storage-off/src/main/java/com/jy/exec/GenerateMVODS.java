package com.jy.exec;

import java.util.List;

import com.jy.bean.loadHiveBean.HiveDataBase;
import com.jy.bean.loadHiveBean.HiveMetastore;
import com.jy.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import com.jy.util.DateUtils;
import com.jy.util.XmlUtil;

public class GenerateMVODS {
	
	public static void main(String[] args) {
		String path = "C:/Users/Administrator/Desktop/文件/20190321/HdfsLoadToHiveAppConfiguration.xml";

		HiveMetastore hiveODSMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(path, null); // 获取近源数据应用层配置文件相关信息
		
		HiveDataBase<HiveODSDataSet> odsdataBase = hiveODSMetastore.getHiveDataBaseList().get(0);
		
		List<HiveODSDataSet> hiveODSdataSetList = odsdataBase.getHiveDataSetList();
		
//		String hive_ods_db_location = JedisOperation.getForMap(Constants.HIVE_DB_LOCATION, odsdataBase.getEnnameH()); // 获取hive-db的location路径
String hive_ods_db_location = "/app_hive/warehouse/product_ods.db";
		
		String mvCommand = "hdfs dfs -mv ";
		String bakDir = "/jlc/ods_hive_bak";
		
		System.out.println("------START------");
		for(HiveODSDataSet odsDataSet : hiveODSdataSetList) {
			
			String command = mvCommand + hive_ods_db_location + "/" + odsDataSet.getEnnameH() + " " + bakDir + "/" + DateUtils.getYesterdayDate() + "/" + odsdataBase.getEnnameH() + "/";
			System.out.println(command);
		}
		System.out.println("------END------");
	}

}
