package com.jy.test.pgsql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;

import com.jy.constant.Constants;

public class PGsqlTesrt {

	public static void main(String[] args) {
		
		Connection c = null;
		
		
		Statement stmt = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://10.103.27.190:5432/stock_market", "stock_rw", "MTRI8bCGR9kO8LEQukye");
			c.setAutoCommit(false);
			System.out.println("Opened database successfully");
			
			DatabaseMetaData dbMeta = c.getMetaData();
			String dbProductName = dbMeta.getDatabaseProductName().toUpperCase();
			System.out.println("------------" + dbProductName);

			stmt = c.createStatement();
			ResultSet resultSet = stmt.executeQuery("SELECT * FROM cash_flow_data");
			while (resultSet.next()) {
				ResultSetMetaData rsmd = resultSet.getMetaData();
				int columnCount = rsmd.getColumnCount(); // 总列数
				
				StringBuffer titleBuf = new StringBuffer();
				StringBuffer dataBuf = new StringBuffer();
				
				for(int i = 1; i <= columnCount; i++) {
					titleBuf.append(rsmd.getColumnName(i).toLowerCase() + Constants.SPECIALCHAR);
					
					String data = resultSet.getString(i);
					if(StringUtils.isBlank(data) || "null".equals(data) || "NULL".equals(data)) {
						data = "";
					}
					dataBuf.append(data.trim() + Constants.U0001);
				}
				
				String title = titleBuf.substring(0, titleBuf.length() - 1);
				
				String str = dataBuf.substring(0, dataBuf.length() - 1);
				
				if(str.contains(Constants.LINE_N)) {
					str = str.replaceAll(Constants.LINE_N, "");
				}
				if(str.contains(Constants.LINE_R)) {
					str = str.replaceAll(Constants.LINE_R, "");
				}
				
				String data = str;
				
				// 打印数据
				System.out.println(title);
				System.out.println(data);
				System.out.println("========================");
			}
			resultSet.close();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Operation done successfully");

	}
}
