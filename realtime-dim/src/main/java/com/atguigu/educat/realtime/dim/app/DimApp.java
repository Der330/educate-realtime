package com.atguigu.educat.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp {
    public static void main(String[] args) {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10010, 4);

        DataStreamSource<String> sourceDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_DB), WatermarkStrategy.noWatermarks(), "DimApp");

        //转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS=sourceDS.map(JSON::parseObject);

        //




    }
}
