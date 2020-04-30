package com.jy.hive;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 *   演示了通过java jdbc 操作hive  ，一般企业环境不会这么做 ，hive 目的是去java 编程能力
 *   京东等企业是通过shell or python  封装 hive -e sql 命令进行数据操作
 *   需要在hive 节点启动 hive --service hiveserver2&
 **/

public class HiveUtil {

    private static Connection conn = null;
    private static Statement statement = null;

    static {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://linux01:10000/default";
        String user = "hadoop"; //一般情况下可以使用匿名的方式，在这里使用了root是因为整个Hive的所有安装等操作都是root
        String password = "";


        try {
            Class.forName(driver); // 加载JDBC驱动
            conn = DriverManager.getConnection(url, user, password); // 通过JDBC建立和Hive的连接器，默认端口是10000，默认用户名和密码都为空
            statement = conn.createStatement(); // 创建Statement句柄，基于该句柄进行SQL的各种操作
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取PreparedStatement
     * @param sql
     * @return
     * @throws Exception
     */
    private static PreparedStatement getPreparedStatement(String sql) throws Exception {
        return conn.prepareStatement(sql);
    }

    /**
     * 获取表的schema
     * @param table
     * @return
     * @throws Exception
     */
    public static List<String> getSchema(String hive_db, String table) throws Exception {
        String sql = "describe " + hive_db + "." + table;

        ResultSet resultSet = statement.executeQuery(sql);

        List<String> schemaList = new ArrayList<String>();
        while (resultSet.next()) {
            schemaList.add(resultSet.getString(1));
        }
        return schemaList;
    }

    /**
     *  添加列
     * @param column
     * @param type
     * @return
     */
    public static boolean addColumn(String hive_db, String tableName, String column, String type) {
       String sql = "alter table " + hive_db + "." + tableName + " add columns(" + column + " " + type + ")";

       try {
           statement.execute(sql);
           return true;
       } catch(Exception e) {
           e.printStackTrace();
           return false;
       }
    }

    public static void main(String[] args) {
        try {
            boolean result = HiveUtil.addColumn("default", "people", "describable", "string");
            System.out.println("添加列：" + result);

            List<String> schemaList = HiveUtil.getSchema("default", "people");
            for(String column : schemaList) {
                System.out.println(column);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}