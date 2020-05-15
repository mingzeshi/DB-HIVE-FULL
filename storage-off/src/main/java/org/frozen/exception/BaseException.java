package org.frozen.exception;

public class BaseException extends RuntimeException {
	
	protected BaseException(String code, String msg) {
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

}
