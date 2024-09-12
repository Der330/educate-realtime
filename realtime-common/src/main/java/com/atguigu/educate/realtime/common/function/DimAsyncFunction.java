package com.atguigu.educate.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.HbaseUtil;
import com.atguigu.educate.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    private AsyncConnection hbaseConn;
    private StatefulRedisConnection<String, String> redisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HbaseUtil.getHBaseAsyncConnection();
        redisConn = RedisUtil.getRedisAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHBaseAsyncConnection(hbaseConn);
        RedisUtil.closeRedisAsyncConnection(redisConn);
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        JSONObject jsonObj = RedisUtil.getDataByAsync(redisConn, getTableName(), getRowKey(t));
        if (jsonObj == null) {
            jsonObj = HbaseUtil.getRowByAsync(hbaseConn, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(t), JSONObject.class);
            if (jsonObj == null) {
                System.out.println("无法找到匹配的维度数据");
            } else {
                RedisUtil.putDataByAsync(redisConn, getTableName(), getRowKey(t), jsonObj);
            }
        }
        if (jsonObj != null) {
            addDims(t, jsonObj);
        }
        resultFuture.complete(Collections.singleton(t));
    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        throw new RuntimeException("异步操作超时");
    }

    public abstract void addDims(T t, JSONObject jsonObj);

    public abstract String getTableName();

    public abstract String getRowKey(T t);
}
