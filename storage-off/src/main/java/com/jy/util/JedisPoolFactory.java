package com.jy.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPool;

import com.jy.constant.Constants;


public class JedisPoolFactory{

    private static final int poolConfigMaxTotal = 300;
    private static final int poolConfigMaxIdle = 30;
    private static final boolean poolConfigTestOnReturn = false;
    private static final boolean poolConfigTestOnBorrow = false;
    private static final boolean poolConfigTestWhileIdle = true;
    private static final boolean poolConfigBlockOnExhausted = false;
    private static final int timeout = 2000;

    private static JedisPool jedis = null;

    public static synchronized  JedisPool getInstance() throws Exception {

        if(jedis == null){
            jedis = getObject();
        }
        return jedis;
    }

    private static JedisPool getObject() throws Exception {

//        if (StringUtils.isBlank(Constants.REDIS_HOST)) {
//            throw new IllegalArgumentException("host should not be empty");
//        }
    	
    	/**
    	 * 生产
    	 */
        System.out.println(Constants.REDIS_HOST + "-" + Constants.REDIS_PORT + "-" + timeout + "-" + Constants.PASSWORD);
        return new JedisPool(buildDefaultPoolConfig(), Constants.REDIS_HOST, Constants.REDIS_PORT, timeout, Constants.PASSWORD);

        /**
         * 测试
         */
//        System.out.println(Constants.REDIS_HOST + "-" + Constants.REDIS_PORT + "-" + timeout);
//        return new JedisPool(buildDefaultPoolConfig(), Constants.REDIS_HOST, Constants.REDIS_PORT, timeout);
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
