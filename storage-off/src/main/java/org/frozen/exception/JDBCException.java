package org.frozen.exception;

public class JDBCException extends BaseException  {

	protected JDBCException(String code, String msg) {
		super(code, msg);
	}
	
	// ==========异常定义==========
	
	public static final JDBCException NO_CONNECTION_EXCEPTION = new JDBCException("50001", "无JDBC连接");
	

}
