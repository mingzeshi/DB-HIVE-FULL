package com.jy.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.jy.constant.Constants;
import com.jy.util.JDBCUtil;


public class TestJDBC {
	
	public static void main(String[] args) {
		try {
			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://10.10.231.183:13008/jianlc_asset";
			String username = "data-reader";
			String password = "qNBgNQxQR6gVrtug";
			Connection conn = JDBCUtil.getConn(driver, url, username, password);
			
			String sql = "select projectInfo from ast_loan_info where id = 7940";
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			
			while (rs.next()) {
				String str = rs.getString(1);
//				System.out.println(str.contains("\n"));
//				str = str.replaceAll("\n", "");
////				str = str.replaceAll("\r", "");
////				str = str.replaceAll("\t", "");
//				System.out.println(str.contains("\n"));
//				System.out.println(str.contains("\r"));
//				System.out.println(str);
//				
//				System.out.println("aaa\naaa".replaceAll("\n", ""));
				
				
				
				if(str.contains(Constants.LINE_N)) {
					str = str.replaceAll(Constants.LINE_N, "");
				}
				if(str.contains(Constants.LINE_R)) {
					str = str.replaceAll(Constants.LINE_R, "");
				}
				
				System.out.println(str);
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
