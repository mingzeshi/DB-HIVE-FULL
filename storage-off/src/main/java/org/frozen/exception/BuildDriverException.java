package org.frozen.exception;

/**
 * 解析XML配置项文件异常信息
 *
 */
public class BuildDriverException extends RuntimeException {

	public BuildDriverException(String code, String msg) {
		this.code = code;
		this.msg = msg;
	}

	private String code;
	private String msg;

	public String getCode() {
		return code;
	}

	public String getMsg() {
		return msg;
	}
	
	// ======异常定义======
	
	public static final BuildDriverException NO_EXPORT_CONFIG = new BuildDriverException("1001", "无数据输出-配置项");
	public static final BuildDriverException NO_HIVE_TAB_CONFIG = new BuildDriverException("1002", "无数数据输出-HIVE表配置项");
}
