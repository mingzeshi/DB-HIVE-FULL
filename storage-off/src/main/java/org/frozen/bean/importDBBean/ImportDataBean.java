package org.frozen.bean.importDBBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import org.frozen.constant.Constants;

/**
 * @author Administrator
 *
 */
public class ImportDataBean implements Writable,DBWritable {
	
	private String db;
	private String table;
	private String title;
	private String data;
		
	public ImportDataBean() {
	}
	
	public ImportDataBean(String db, String table, String title, String data) {
		this.db = db;
		this.table = table;
		this.title = title;
		this.data = data;
	}

	// -----------------------------------
	
	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
	
	// -----------------------------------

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
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

		this.title = titleBuf.substring(0, titleBuf.length() - 1);
		
		String str = dataBuf.substring(0, dataBuf.length() - 1);
		
		if(str.contains(Constants.LINE_N)) {
			str = str.replaceAll(Constants.LINE_N, "");
		}
		if(str.contains(Constants.LINE_R)) {
			str = str.replaceAll(Constants.LINE_R, "");
		}
		this.data = str;
		
//		StringBuffer dataBuf = new StringBuffer();
//		for(int i = 1; i <= columnCount; i++) {			
//			String columnname = rsmd.getColumnName(i);
//			String columntype = rsmd.getColumnTypeName(i);
//			String data = resultSet.getString(i);
			/*
			dataBuf.append("{");
			
			dataBuf.append("\"" + Constants.COLUMNNAME + "\"");
			dataBuf.append(":");
			dataBuf.append("\"" + columnname + "\"");
			
			dataBuf.append(",");
			
			dataBuf.append("\"" + Constants.COLUMNTYPE + "\"");
			dataBuf.append(":");
			dataBuf.append("\"" + columntype + "\"");
			
			dataBuf.append(",");
			
			if(StringUtils.isBlank(data) || "null".equals(data) || "NULL".equals(data)) {
				data = "";
			}
			dataBuf.append("\"" + Constants.DATA + "\"");
			dataBuf.append(":");
			dataBuf.append("\"" + data + "\"");
			
			dataBuf.append("}");
			
			dataBuf.append(",");
			*/
			/*
			dataBuf.append("{");
			
			dataBuf.append("\"" + columnname + "\"");
			dataBuf.append(":");
			if(StringUtils.isBlank(data) || "null".equals(data) || "NULL".equals(data)) {
				data = "";
			}
			dataBuf.append("\"" + data + "\"");
			
			dataBuf.append("}");
			
			dataBuf.append(",");
			*/
//		}
		
//		this.data = "[" + dataBuf.substring(0, dataBuf.length() - 1) + "]";
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
		String[] dataArray = this.data.split(Constants.U0001);
		
		for(int i = 0; i < dataArray.length; i++) {
			statement.setString(i + 1, dataArray[i]);
		}
		
		/*
		JSONArray jsonArray = JSONArray.fromObject(this.data);
		for(int i = 0; i < jsonArray.size(); i++) {
			JSONObject jsonObject = jsonArray.getJSONObject(i);
			statement.setString(i + 1, jsonObject.get(Constants.DATA).toString());
		}
		*/
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.db = dataInput.readUTF();
		this.table = dataInput.readUTF();
		this.data = dataInput.readUTF();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.db);
		dataOutput.writeUTF(this.table);
		dataOutput.writeUTF(this.data);
	}
	
}
