package com.atguigu.educate.realtime.dwd.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.DateFormatUtil;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DwdUserLoginInfo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10015, 4);

        DataStreamSource<String> topicLogDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_LOG, "DwsUserBackWindow"), WatermarkStrategy.noWatermarks(), "KafkaSource");

        topicLogDS.map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }))
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, String>() {

                            ValueState<String> lastLoginData;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor=new ValueStateDescriptor<>("lastLoginData", String.class);
                                lastLoginData = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                                //用户域登录行为事实表：用户ID，登陆时间，登录时间戳，是否首次登陆，与上次登录日期差
                                JSONObject loginInfoObj=new JSONObject();

                                //用户ID
                                String uid=jsonObject.getJSONObject("common").getString("uid");
                                //登录时间戳
                                Long ts=jsonObject.getLong("ts");
                                //登陆日期
                                String loginDate=DateFormatUtils.format(ts, "yyyy-MM-dd");
                                //是否新用户
                                Long isNew=jsonObject.getJSONObject("common").getLong("is_new");
                                //与上次登录日期的天数差
                                Long lastDateCut = 0L;

                                //状态不为空
                                    //登录日期 < 上次日期   -   乱序（不处理）
                                    //登录日期 = 上次日期   -   一天内重复登陆，不处理
                                    //登录日期 > 上次日期   -   求日期差，更新状态日期
                                //状态为空：首日登录，更新状态，日期差为0

                                String stateValue=lastLoginData.value();



                                if (stateValue != null) {
                                    //判断与上次登录的日期差
                                    if (DateFormatUtil.dateToTs(loginDate) > DateFormatUtil.dateToTs(stateValue)){
                                        lastDateCut = DateFormatUtil.getDateCutOfDays(loginDate,stateValue);
                                    }
                                }

                                if (!loginDate.equals(stateValue)){
                                    //用户ID，登陆时间，登录时间戳，是否首次登陆，与上次登录日期差
                                    loginInfoObj.put("uid",uid);
                                    loginInfoObj.put("loginDate",loginDate);
                                    loginInfoObj.put("ts",ts);
                                    loginInfoObj.put("isNew",isNew);
                                    loginInfoObj.put("lastDateCut",lastDateCut);
                                    collector.collect(loginInfoObj.toJSONString());
                                }

                                lastLoginData.update(loginDate);
                            }
                        }
                )
                .sinkTo( FlinkKafkaUtil.getKafkaSink("dwd_user_login_info"));

        env.execute();



    }
}
