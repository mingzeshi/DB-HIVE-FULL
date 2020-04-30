package com.jy.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import com.jy.constant.Constants;

/**
 * redis操作
 */
public class JedisOperation {

    public static final List<Object> emptyList = new ArrayList<Object>();

    // ----------------------------hashmap
    public static List<Object> putForMap(String key, Map<String, String> valueMap) {
        return putForMap(key, valueMap, Constants.EXPIRE_SECONDS);
    }

    public static List<Object> putForMap(String key, Map<String, String> valueMap, int expireSecond) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();
                for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                    pipeline.hset(key, entry.getKey(), entry.getValue());
                    if (expireSecond != -1) {
                        pipeline.expire(key, expireSecond);
                    }
                }
                return pipeline.syncAndReturnAll();
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return emptyList;
    }

    public static List<Object> getForMap(String key, Set<String> fields) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();
                for (String field : fields) {
                    pipeline.hget(key, field);
                }
                return pipeline.syncAndReturnAll();
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return emptyList;
    }

    //获取key对应的Map结果
    public static Map<String, String> getForMap(String key) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                return jedis.hgetAll(key);
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return new HashMap<String, String>();
    }

    //向Map中插入一条key=value数据
    public static void putForMap(String key, String field, String value, int expireSecond) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                jedis.hset(key, field, value);
                if (expireSecond != -1) {
                    jedis.expire(key, expireSecond);
                }
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
    }

    //获取map对象的一个key对应的值
    public static String getForMap(String key, String field) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                return jedis.hget(key, field);
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return "";
    }

    //增量的同步redis中value
    public static List<Object> putForMapHincr(String key, Map<String, Double> valueMap, int expireSecond) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();
                for (Map.Entry<String, Double> entry : valueMap.entrySet()) {
                    pipeline.hincrByFloat(key, entry.getKey(), entry.getValue());
                    if (expireSecond != -1) {
                        pipeline.expire(key, expireSecond);
                    }
                }
                return pipeline.syncAndReturnAll();
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return emptyList;
    }

    //-----------------------------set
    public static List<Object> putForSet(String key, Set<String> valueSet) {
        return putForSet(key, valueSet, Constants.EXPIRE_SECONDS);
    }

    public static List<Object> putForSet(String key, Set<String> valueSet, int expireSecond) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();
                for (String value : valueSet) {
                    pipeline.sadd(key, value);
                    if (expireSecond != -1) {
                        pipeline.expire(key, expireSecond);
                    }
                }
                return pipeline.syncAndReturnAll();
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return emptyList;
    }

    // 设置set的一个value,如果设置成功,则返回true
    public static boolean addValueInSet(String key, String value) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                return 0 != jedis.sadd(key, value);//如果元素已经成功插入,即以前该value不存在,则输出1,如果以前value存在,则返回0
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return false;
    }

    // 获取
    public static Set<String> getSet(String key) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                return jedis.smembers(key);
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return new HashSet<String>();
    }

    // 移除set的一个或多个value,如果设置成功,则返回true
    public static boolean removeValueInSet(String key, String ... value) {
        for (int i = 0; i < Constants.MAX_ATTEMPTS; ++i) {
            JedisPool jedisPool = null;
            Jedis jedis = null;
            try {
                jedisPool = JedisPoolFactory.getInstance();
                jedis = jedisPool.getResource();
                return 0 != jedis.srem(key, value);//如果元素已经成功插入,即以前该value不存在,则输出1,如果以前value存在,则返回0
            } catch (JedisException e) {
                e.printStackTrace();
                if (jedis != null) {
                    Client client = jedis.getClient();
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.returnResource(jedis);
            }
        }
        return false;
    }
    //-----------------------------zset

}
