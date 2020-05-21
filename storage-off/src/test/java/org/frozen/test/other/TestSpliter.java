package org.frozen.test.other;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.frozen.util.JDBCUtil;

public class TestSpliter {
	
	
	public static void main(String[] args) {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://10.11.100.1:13006/snb_live";
		String username = "snbread";
		String password = "snb_read2021";
		
		String[] tableArray = new String[] {
				"event_receive",
				"event_send",
				"user_daily_income",
				"flow_number",
				"order_rebalance_summary_detail",
				"order_rebalance_failure_record",
				"portfolio_daily_income",
				"order_info",
				"order_dividend",
				"order_rebalance",
				"order_invest_detail",
				"order_rebalance_auth"
		};
		
		try {
			Connection connection = JDBCUtil.getConn(driver, url, username, password);
			Statement statement = connection.createStatement();
			ResultSet results = null;
			for(String table : tableArray) {
				results = statement.executeQuery(getBoundingValsQuery(table, "id", "create_time > date_sub(curdate(),interval 1 day) or update_time > date_sub(curdate(),interval 1 day)"));
				results.next();
				
				long minVal = results.getLong(1);
				long maxVal = results.getLong(2);
				
				List<Long> splitPoints = split(20000L, minVal, maxVal);
				
				System.out.println(table + ":" + splitPoints.size());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public static String getBoundingValsQuery(String tableName, String splitCol, String conditions) {
		conditions = null; 

		StringBuilder query = new StringBuilder();

		query.append("SELECT MIN(").append(splitCol).append("), ");
		query.append("MAX(").append(splitCol).append(") FROM ");
		query.append(tableName);
		if (StringUtils.isNotBlank(conditions)) {
			query.append(" WHERE ( " + conditions + " )");
		}

		return query.toString();
	}
	
	public static List<Long> split(long batchCount, long minVal, long maxVal) throws SQLException {

		List<Long> splits = new ArrayList<Long>();

		long curVal = minVal;

		while (curVal <= maxVal) {
			splits.add(curVal);
			curVal += batchCount;
		}

		if (splits.get(splits.size() - 1) != maxVal || splits.size() == 1) {
			splits.add(maxVal);
		}

		return splits;
	}

}
