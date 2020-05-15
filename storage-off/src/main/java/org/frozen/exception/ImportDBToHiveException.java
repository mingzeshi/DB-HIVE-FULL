package org.frozen.exception;

/**
 * 解析XML配置项文件异常信息
 *
 */
public class ImportDBToHiveException  extends BaseException  {

	public ImportDBToHiveException(String code, String msg) {
		super(code, msg);
	}
	
	// ======异常定义======
	
	public static final ImportDBToHiveException NO_EXPORT_CONFIG_EXCEPTION = new ImportDBToHiveException("20001", "无数据输出-配置项");
	public static final ImportDBToHiveException NO_HIVE_TAB_CONFIG_EXCEPTION = new ImportDBToHiveException("20002", "无数据据输出-HIVE表配置项");
}
