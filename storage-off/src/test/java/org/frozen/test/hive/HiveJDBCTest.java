package org.frozen.test.hive;

import java.sql.Connection;

import org.frozen.hive.HiveUtil;

public class HiveJDBCTest {
	
	public static void main(String[] args) {
		
		try {
			HiveUtil hiveUtil = HiveUtil.getInstance();
			String DRIVER = "org.apache.hive.jdbc.HiveDriver";
			String URL = "jdbc:hive2://slavenode163.data.test.ds:10000/default;ssl=true;sslTrustStore=C:/Users/Administrator/Desktop/文件/20200511/cm-auto-global_truststore.jks;trustStorePassword=R2j5DkgDZVvjTziNMNjuyvSB9pVBeFzRkKCmU4LfBuN";
			String USERNAME = "";
			String PASSWORD = "";
			Connection connection = hiveUtil.createOrGetConnection(DRIVER, URL, USERNAME, PASSWORD);
			
			String sql = "create table tmp.hahaha(id string, name string)";
//			
			connection.createStatement().execute(sql);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
