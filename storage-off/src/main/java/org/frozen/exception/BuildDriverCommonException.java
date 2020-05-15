package org.frozen.exception;

public class BuildDriverCommonException extends BaseException {

	protected BuildDriverCommonException(String code, String msg) {
		super(code, msg);
	}

	// ======异常定义======
	
	public static final BuildDriverCommonException NO_CONFIG_FILE_EXCEPTION = new BuildDriverCommonException("10001", "找不到：configFile参数，例：-DconfigFile=/config/configfile.xml");
}
