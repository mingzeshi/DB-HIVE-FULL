package com.jy.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {

    public static void main(String[] args) {
//        TestJDBC.getAll("product_storage", "mobile_authen");
//        getAllTable();
    }

    
    
    public static Connection getConn(String driver, String url, String username, String password) {
//        String driver = "com.mysql.jdbc.Driver";
//        String url = "jdbc:mysql://10.11.100.1:13007/product";
//        String username = "data-reader";
//        String password = "qNBgNQxQR6gVrtug";

        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    
    /**
     * @描述：获取hive表columns信息
     * @param conn
     * @param db
     * @param table
     * @return
     */
    public static List<String> getHiveTabColumns(Connection conn, String db, String table) {
    	List<String> fieldList = new ArrayList<String>();
    	
    	String sql = "select c.column_name from columns_v2 c, sds s, dbs d, tbls t where c.cd_id = s.cd_id and s.sd_id = t.sd_id and t.db_id = d.db_id and d.name = '" + db + "' and t.tbl_name = '" + table + "' order by c.integer_idx";
//String sql = "select c.column_name from COLUMNS_V2 c, SDS s, DBS d, TBLS t where c.cd_id = s.cd_id and s.sd_id = t.sd_id and t.db_id = d.db_id and d.name = '" + db + "' and t.tbl_name = '" + table + "' order by c.integer_idx";
    	PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                fieldList.add(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return fieldList;
    }
    
    /**
     * @描述：获取hive db location
     * @param conn
     * @param db
     * @param table
     * @return
     */
    public static String getHiveDBLocation(Connection conn, String db) {
    	String location = "";
    	String sql = "select db_location_uri from dbs where name = '" + db + "'";
    	PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
            	location = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return location;
    }
    

    private static void getAllTable(Connection conn) {
//        Connection conn = getConn();
        String sql = "show tables";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String table = rs.getString(1);
//                getAll("product_storage", table);
                System.out.println(table);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void getAll(Connection conn, String db, String table) {
//        Connection conn = getConn();
        String sql = "desc " + table;
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            StringBuffer buf = new StringBuffer();
            while (rs.next()) {
                String type = rs.getString(2);
                String finalType = "string";
                if(type.startsWith("int")) {
                    finalType = "int";
                } else if(type.startsWith("bigint")) {
                    finalType = "bigint";
                } else if(type.startsWith("decimal")) {
                    finalType = "double";
                }

                buf.append(rs.getString(1) + " " + finalType + ",");
            }
            System.out.println("create table if not exists " + db + "." + table + "(" + buf.toString().substring(0, buf.length() - 1) + ") partitioned by (part_log_day string, hour string, min string);");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
