package org.frozen.hive;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.frozen.exception.JDBCException;

public class HiveUtil {
	
	private static volatile HiveUtil hiveUtil;

    public HiveUtil() {}

	public static HiveUtil getInstance() throws Exception {
    	if(hiveUtil == null) {
    		synchronized (HiveUtil.class) {
    			if(hiveUtil == null) {
    				hiveUtil = new HiveUtil();
    			}
			}
    	}
    	return hiveUtil;
    }

    /**
     * 获取PreparedStatement
     * @param sql
     * @return
     * @throws Exception
     */
    public PreparedStatement executeSQL(Connection connection, String sql) throws Exception {
    	
    	if(connection == null)
    		throw JDBCException.NO_CONNECTION_EXCEPTION;
    	
        return connection.prepareStatement(sql);
    }

    /**
     * 获取表的schema
     * @param table
     * @return
     * @throws Exception
     */
    public List<String> getSchema(Connection connection, String hive_db, String table) throws Exception {
    	if(connection == null)
    		throw JDBCException.NO_CONNECTION_EXCEPTION;
    	
        String sql = "describe " + hive_db + "." + table;

        ResultSet resultSet = connection.createStatement().executeQuery(sql);

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
	public boolean addColumn(Connection connection, String hive_db, String tableName, String column, String type) {
		if (connection == null)
			throw JDBCException.NO_CONNECTION_EXCEPTION;

		String sql = "alter table " + hive_db + "." + tableName + " add columns(" + column + " " + type + ")";

		try {
			connection.createStatement().execute(sql);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
    
    /**
     * 获取链接
     * @return
     * @throws Exception
     */
    public Connection createOrGetConnection(String driver, String url, String user, String password) throws Exception {
    	Class.forName(driver); // 加载JDBC驱动
    	return DriverManager.getConnection(url, user, password); // 通过JDBC建立和Hive的连接器，默认端口是10000，默认用户名和密码都为空
    }
}