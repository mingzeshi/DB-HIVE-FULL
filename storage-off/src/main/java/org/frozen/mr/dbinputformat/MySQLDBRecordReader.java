package org.frozen.mr.dbinputformat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class MySQLDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

	public MySQLDBRecordReader(CustomDBInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table) throws SQLException {
		super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
	}

	// Execute statements for mysql in unbuffered mode.
	protected ResultSet executeQuery(String query) throws SQLException {
		PreparedStatement statement = getConnection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read
													// row-at-a-time.
		setStatement(statement); // save a ref for cleanup in close()
		return statement.executeQuery();
	}
}
