package org.frozen.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPool;

import org.frozen.constant.Constants;


public class JedisPoolFactory{

    private static final int poolConfigMaxTotal = 300;
    private static final int poolConfigMaxIdle = 30;
    private static final boolean poolConfigTestOnReturn = false;
    private static final boolean poolConfigTestOnBorrow = false;
    private static final boolean poolConfigTestWhileIdle = true;
    private static final boolean poolConfigBlockOnExhausted = false;
    private static final int timeout = 2000;

    private static JedisPool jedis = null;

    public static JedisPool getInstance(String redis_host, Integer redis_port, String redis_password) throws Exception {
        if(jedis == null) {
        	synchronized (jedis) {
        		if(jedis == null) {
        			System.out.println(redis_host + "-" + redis_port + "-" + timeout + "-" + redis_password);
        			jedis = new JedisPool(buildDefaultPoolConfig(), redis_host, redis_port, timeout, redis_password);
        		}
			}
        }
        return jedis;
    }

    private static GenericObjectPoolConfig buildDefaultPoolConfig() {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setTestOnReturn(poolConfigTestOnReturn);
        poolConfig.setTestOnBorrow(poolConfigTestOnBorrow);
        poolConfig.setTestWhileIdle(poolConfigTestWhileIdle);
        poolConfig.setBlockWhenExhausted(poolConfigBlockOnExhausted);
        poolConfig.setMaxIdle(poolConfigMaxIdle);
        poolConfig.setMaxTotal(poolConfigMaxTotal);
        return poolConfig;
    }
}
