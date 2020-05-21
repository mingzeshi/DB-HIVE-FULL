package org.frozen.exception;

public class RedisException extends BaseException {

	protected RedisException(String code, String msg) {
		super(code, msg);
	}
	
	// ==========异常定义==========
	
	public static RedisException JEDISOPERATION_NULL_EXCEPTION = new RedisException("60001", "JedisOperation工具类NULL");

}
