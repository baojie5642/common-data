package net.common.data.redis;

import net.common.data.redis.client.BaseShardedJedisPipeline;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p/>
 * User : krisibm@163.com
 * Date: 2015/7/1
 * Time: 10:30
 */
public interface IRedis {

    /**
     * Set操作：获取Set的成员数量
     *
     * @param key
     * @return
     */
    Long scard(final String key);

    /**
     * Set操作：删除String对象
     *
     * @param key
     * @param member
     * @return
     */
    Long srem(String key, String member);

    /**
     * Set操作：增加String对象
     *
     * @param key
     * @param member
     * @return
     */
    Long sadd(String key, String member);

    /**
     * Set操作：增加一个对象成员
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    <T extends Serializable> Long saddObject(String key, T value);

    /**
     * Set操作：查询Set中某成员是否存在
     *
     * @param key
     * @param member
     * @return
     */
    boolean sismember(final String key, final String member);

    /**
     * Set操作：查询Set中某成员是否存在 (Serializable成员 ：Object类型)
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    <T extends Serializable> Boolean sismemberObject(String key, T value);

    /**
     * Set操作：获取所有Set成员（String类型成员）
     *
     * @param key
     * @return
     */
    Set<String> smembers(final String key);

    /**
     * Set操作：获取所有Set成员(Serializable成员 ：Object类型)
     *
     * @param key
     * @param <T>
     * @return
     */
    <T extends Serializable> Set<T> smembersObject(String key);


    /**
     * Map操作：为map中某个key的值incr
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    Long hincr(String key, String field, long value);

    /**
     * Map操作：为map中某个key的值incr , 带失效时间
     *
     * @param key
     * @param field
     * @param value
     * @param expireSeconds
     * @return
     */
    void hincr(String key, String field, long value, int expireSeconds);

    /**
     * Map操作：获得某个map中的指定数据
     *
     * @param key
     * @param field
     * @return
     */
    String hget(String key, String field);

    /**
     * Map操作：获得某个map中的指定数据
     *
     * @param key
     * @param field
     * @return
     */
    byte[] hget(byte[] key, byte[] field);

    /**
     * Map操作：查看哈希表key中，给定域field是否存在
     *
     * @return
     */
    boolean hexists(String key, String field);

    /**
     * Map操作：获得某个map中所有的数据
     *
     * @param key
     * @return
     */
    Map<String, String> hgetAll(String key);

    /**
     * Map操作: 获取多个field
     *
     * @param key
     * @param fields
     * @return
     */
    List<String> hmget(String key, String... fields);

    /**
     * Map操作：获得哈希表中key对应的所有field
     *
     * @param key
     * @return
     */
    Set<String> hkeys(String key);

    /**
     * Map操作：获得哈希表中key对应的所有values
     *
     * @param key
     * @return
     */
    List<String> hvals(String key);

    /**
     * Map操作：设置某个map中的指定数据
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    Long hset(String key, String field, String value);

    /**
     * Map操作：设置某个map中的指定数据
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    Long hset(byte[] key, byte[] field, byte[] value);

    /**
     * Map操作： 返回对应的field的数量
     *
     * @param key
     * @return
     */
    Long hlen(String key);

    /**
     * Map操作：删除哈希表key中的指定域，不存在的域将被忽略
     *
     * @param key
     * @param field
     * @return
     */
    Long hdel(String key, String field);

    /**
     * Map操作：删除哈希表key中的指定域，不存在的域将被忽略
     *
     * @param key
     * @param field
     * @return
     */
    Long hdel(byte[] key, byte[] field);

    /**
     * 删除一个key值的value
     *
     * @param key
     * @return
     */
    Long del(String key);

    /**
     * 添加对象
     *
     * @param key
     * @param value
     * @return
     */
    <T extends Serializable> void setObject(String key, T value, int expireSeconds);

    /**
     * 查询对象
     *
     * @param key
     * @return
     */
    Object getObject(String key, final int expireSecond);

    /**
     * String操作：将字符串值 value 关联到 key
     *
     * @param key
     * @param value
     * @return
     */
    String set(String key, String value, int expireSeconds);

    /**
     * String操作：返回 key 所关联的字符串值
     *
     * @param key
     * @return
     */
    String get(String key);

    /**
     * 判断指定key是否存在
     *
     * @param key
     * @return
     */
    Boolean exists(String key);

    /**
     * String操作：指定字段的值＋1
     *
     * @param key
     * @return
     */
    Long incr(String key);

    /**
     * 指定字段的值＋1，并设置过期时间
     *
     * @param key
     * @param seconds
     * @return
     */
    Long incr(String key, int seconds);

    /**
     * List操作：返回列表长度
     *
     * @param key
     * @return
     */
    Long llen(String key);


    /**
     * 从列表首部插入值
     *
     * @param key
     * @param string
     * @return
     */
    Long rpush(String key, String string);

    /**
     * List操作：从列表尾部插入值
     *
     * @param key
     * @param string
     * @return
     */
    Long lpush(String key, String string);

    /**
     * List操作：取出指定长度的内容，－1表示最后一位，-2表示倒数第二位
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    List<String> lrange(String key, long start, long end);

    /**
     * List操作：获得指定位置的内容
     *
     * @param key
     * @param index
     * @return
     */
    String lindex(String key, long index);

    /**
     * List操作：从列表首部删除一个元素
     *
     * @param key
     * @return
     */
    String lpop(String key);

    /**
     * List操作：从列表尾部删除一个元素
     *
     * @param key
     * @return
     */
    String rpop(String key);


    /**
     * List操作：对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    String ltrim(String key, long start, long end);

    /**
     * List操作：将列表key下标为index的元素的值甚至为value
     *
     * @param key
     * @param index
     * @param value
     * @return
     */
    String lset(String key, long index, String value);

    /**
     * List操作：根据参数count的值，移除列表中与参数value相等的元素，value为0时候都删除，大于零从头部删除，反之从尾部开始删除
     *
     * @param key
     * @param count
     * @param value
     * @return
     */
    Long lrem(String key, long count, String value);


    /**
     * 设置实效时间
     *
     * @param key
     * @param seconds
     * @return
     */
    Long expire(String key, int seconds);

    /**
     * SortSet操作 ：获取数据成员的索引，按照反排序（最大的成员索引最小）
     *
     * @param key
     * @param member
     * @return
     */
    Long zrevrank(String key, String member);

    /**
     * SortSet操作 ：获取set中成员总数
     *
     * @param key
     * @return
     */
    Long zcard(String key);

    /**
     * 使用管道处理多个命令
     *
     * @param baseShardedJedisPipeline
     * @return
     */
    List<Object> pipelined(BaseShardedJedisPipeline baseShardedJedisPipeline);

    /**
     * 根据byte[] key 获得 byte[] 类型数据
     *
     * @param key
     * @return
     */
    byte[] get(byte[] key);

    /**
     * 根据byte[] key 存入 byte[] 类型数据
     *
     * @param key
     * @param value
     * @return
     */
    String set(byte[] key, byte[] value);

    /**
     * 设置基于 byte[] key 的失效时间
     *
     * @param key
     * @param seconds
     * @return
     */
    Long expire(byte[] key, int seconds);

    /**
     * SortSet ： 删除一个成员
     *
     * @param key
     * @param member
     * @return
     */
    Long zrem(String key, String member);

    /**
     * 将指定key的值减1
     *
     * @param key
     * @return
     */
    Long decr(String key);
}
