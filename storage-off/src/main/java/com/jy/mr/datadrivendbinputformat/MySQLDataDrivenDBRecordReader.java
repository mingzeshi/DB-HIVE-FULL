package com.jy.mr.datadrivendbinputformat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MySQLDataDrivenDBRecordReader<T extends DBWritable> extends DataDrivenDBRecordReader<T> {

	public MySQLDataDrivenDBRecordReader(CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table) throws SQLException {
		super(split, inputClass, conf, conn, dbConfig, cond, fields, table, "MYSQL");
	}

  // Execute statements for mysql in unbuffered mode.
  protected ResultSet executeQuery(String query) throws SQLException {
	 PreparedStatement statement = getConnection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//    statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
	 statement.setFetchSize(getConfiguration() == null ? Integer.MIN_VALUE : getConfiguration().getInt("jdbc.fetch.size", Integer.MIN_VALUE));
    return statement.executeQuery();
  }
}
