package com.alibaba.middlware.race.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache<Key, Value> - 缓存数据管理器，线程安全。支持：泛型、缓存过期时间
 *
 * @param <Key>
 *            - 缓存键对象
 * @param <Value>
 *            - 缓存值对象
 */
public class Cache<Key, Value>
{
    /**
     * 缓存容器
     */
    private Map<Key, CacheWrapper<Value>> container = new ConcurrentHashMap<Key, CacheWrapper<Value>>();

    /**
     * 最大缓存容量
     */
    private int maxCacheSize = 100;

    /**
     * 最小移除缓存数量
     */
    private int minRemoveSize = 5;

    /**
     * 过期时间
     */
    private long defaultExpire = 1800L; // 30 分钟

    /**
     * 常量, 100年
     */
    private final static long CenturyExpire = 3155695200L;

    /**
     * 缓存回调对象
     */
    private ICacheCallBack<Key, Value> cacheCallBack = null;

    public Cache()
    {
        this(100, CenturyExpire);
    }

    /**
     * Cache(int maxCacheSize)
     * 
     * @param maxCacheSize
     *            - 最大缓存容量
     */
    public Cache(int maxCacheSize)
    {
        this(maxCacheSize, CenturyExpire, null);
    }

    /**
     * Cache(long expire)
     * 
     * @param defaultExpire
     *            - 缺省过期时间, 单位：秒
     */
    public Cache(long defaultExpire)
    {
        this(100, defaultExpire, null);
    }

    /**
     * Cache(int maxCacheSize, long defaultExpire)
     * 
     * @param maxCacheSize
     *            - 最大缓存容量
     * @param defaultExpire
     *            - 缺省过期时间, 单位：秒
     */
    public Cache(int maxCacheSize, long defaultExpire)
    {
        this(maxCacheSize, defaultExpire, null);
    }

    /**
     * Cache(int maxCacheSize, long defaultExpire, ICacheCallBack<Key, Value>
     * cacheCallBack)
     * 
     * @param maxCacheSize
     *            - 最大缓存容量
     * @param defaultExpire
     *            - 缺省过期时间, 单位：秒
     * @param cacheCallBack
     *            - 缓存回调接口
     */
    public Cache(int maxCacheSize, long defaultExpire, ICacheCallBack<Key, Value> cacheCallBack)
    {
        this.maxCacheSize = maxCacheSize;
        this.defaultExpire = (defaultExpire > 0) ? defaultExpire : CenturyExpire;
        this.cacheCallBack = cacheCallBack;
    }

    /**
     * 取缓存对象
     * 
     * @param key
     * @return
     */
    public Value get(Key key)
    {
        Value cacheValue = null;

        if (key == null)
        {
            return null;
        }

        if (this.container.containsKey(key))
        {
            CacheWrapper<Value> cacheWrapper = this.container.get(key);

            // 检查缓存是否过期
            if (cacheWrapper.absoluteExpiration > this.currentTimestamp())
            {
                // 缓存未过期
                cacheWrapper.count++;
                cacheValue = cacheWrapper.cacheValue;
            }
            else
            {
                // 缓存过期，则移除
                this.container.remove(key);
            }
        }

        // 尝试通过缓存回调方法重新读取对象并缓存
        if (cacheValue == null)
        {
            cacheValue = this.getObjectByCallBack(key);

            if (cacheValue != null)
            {
                this.addCache(key, cacheValue, this.defaultExpire);
            }
        }

        return cacheValue;
    }

    /**
     * 通过回调方法，获得缓存对象。 注意：如果回调方法存在阻塞情况，则将影响到其它线程对缓存的访问，降低缓存的访问性能
     * 
     * @param key
     * @return
     */
    private Value getObjectByCallBack(Key key)
    {
        Value value = null;

        if (this.cacheCallBack != null)
        {
            try
            {
                value = this.cacheCallBack.getObjectCallBack(key);
            }
            catch (Exception ex)
            {
                System.out.println(
                        "Exception: ICacheCallBack.getObjectCallBack(key) - " + ex.getMessage()
                    );
            }
        }

        return value;
    }



    /**
     * 数据对象加入缓存
     * 
     * @param key
     *            - 索引键
     * @param value
     *            - 缓存数据对象
     * @param expire
     *            - 相对过期时间，单位：秒
     */
    private void addCache(Key key, Value value, long expire)
    {
        // 参数检查
        if ((key != null) && (value != null))
        {
            long absoluteExpiration = this.currentTimestamp() + expire;

            // 检查指定键的缓存对象是否存在
            if (!this.container.containsKey(key))
            {
                // 是否需要释放缓存空间
                if (this.container.size() >= this.maxCacheSize)
                {
                    this.release(this.minRemoveSize);
                }

                // 缓存对象不存在，则加入缓存
                CacheWrapper<Value> cacheWrapper = new CacheWrapper<Value>(value, absoluteExpiration);
                this.container.put(key, cacheWrapper);
            }
            else
            {
                // 存在，则修改其绝对过期时间
                CacheWrapper<Value> cacheWrapper = this.container.get(key);
                cacheWrapper.absoluteExpiration = absoluteExpiration;
            }
        }
    }

    /**
     * 清空缓存
     */
    public synchronized void clear()
    {
        this.container.clear();
    }

    /**
     * 缓存对象数量
     * 
     * @return
     */
    public synchronized int size()
    {
        return this.container.size();
    }

    /**
     * 释放缓存空间
     * 
     * @param size
     *            - 释放数量
     */
    private void release(int size)
    {
        ArrayList<Map.Entry<Key, CacheWrapper<Value>>> tmpList = new ArrayList<Map.Entry<Key, CacheWrapper<Value>>>(
                this.container.entrySet());

        // HashMap 按值正排序（从小到大）
        Collections.sort(tmpList, new Comparator<Map.Entry<Key, CacheWrapper<Value>>>()
        {
            public int compare(Entry<Key, CacheWrapper<Value>> x, Entry<Key, CacheWrapper<Value>> y)
            {
                // 正排序（从小到大）
                return (int) (x.getValue().count - y.getValue().count);
            }
        });

        // 移除使用次数最小的缓存数据
        for (int i = 0; (i < size) && (this.container.size() > 0); i++)
        {
            Key key = tmpList.get(i).getKey();
            this.container.remove(key);
        }

    }

    /**
     * 获取当前时间戳
     * 
     * @return
     */
    private long currentTimestamp()
    {
        // 内部使用，不考虑时区问题
        return (System.currentTimeMillis() / 1000L);
    }

    /**
     * CacheWrapper<T> 缓存数据包装类 - 泛型，Cache内部类
     *
     */
    private class CacheWrapper<T>
    {
        private CacheWrapper(T cacheValue, long absoluteExpiration)
        {
            this.cacheValue = cacheValue;
            this.count = 0;
            this.absoluteExpiration = absoluteExpiration;
        }

        /**
         * 访问记数器
         */
        private long count;

        /**
         * 绝对过期时间（时戳）
         */
        private long absoluteExpiration;

        /**
         * 缓存数据值对象
         */
        private T cacheValue;
    }

}
