package org.frozen.exception;

public class TableBalanceException extends BaseException {

	protected TableBalanceException(String code, String msg) {
		super(code, msg);
	}
	
	// ==========异常定义==========
	
	public static final TableBalanceException ARGS_DEFICIENCY_EXCEPTION = new TableBalanceException("40001", "参数不准确");
	public static final TableBalanceException JDBC_CONNECTION_EXCEPTION = new TableBalanceException("40002", "JDBC链接异常");
	public static final TableBalanceException TAB_NOT_EXISTS_EXCEPTION = new TableBalanceException("40003", "配置项中的表不存在");

}
