package com.atguigu.educate.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsUserPageBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public abstract class DwsUserPageProcessFunction<T> extends KeyedProcessFunction<T, JSONObject, DwsUserPageBean> {
    protected ValueState<String> state;
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor=new ValueStateDescriptor<>("state", String.class);
        state = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void close() throws Exception {
        state.clear();
    }

}
