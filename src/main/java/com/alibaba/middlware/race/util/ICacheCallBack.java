package com.alibaba.middlware.race.util;

/**
 * 缓存回调接口定义
 *
 */
public interface ICacheCallBack<Key, Value>
{
    Value getObjectCallBack(Key key);
}
