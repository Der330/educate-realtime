package com.atguigu.educate.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsUserLoginInfoBean;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.test.BeanToJsonStrMapFunction;
import com.atguigu.educate.realtime.common.util.*;
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

/*
    用户域登录行为事实表：用户ID，登陆时间，是否首次登陆，与上次登录日期差
    回流统计思路：在状态中放上次登陆日期，若本次登陆日期与上次登录日期大于7日，则为7日回流用户
 */

public class DwsUserLoginInfo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10025, 4);

        DataStreamSource<String> topicLogDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_START_TAG, "DwsUserLoginInfo"), WatermarkStrategy.noWatermarks(), "KafkaSource");

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
                        new KeyedProcessFunction<String, JSONObject, DwsUserLoginInfoBean>() {

                            ValueState<String> lastLoginData;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor=new ValueStateDescriptor<>("lastLoginData", String.class);
                                lastLoginData = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsUserLoginInfoBean>.Context context, Collector<DwsUserLoginInfoBean> collector) throws Exception {
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
                                    loginInfoObj.put("isNewCount",isNew);
                                    loginInfoObj.put("lastDateCut",lastDateCut);
                                    DwsUserLoginInfoBean loginInfoBean=DwsUserLoginInfoBean.builder()
                                            .uid(uid)
                                            .loginDate(loginDate)
                                            .ts(ts)
                                            .isNewCount(isNew)
                                            .lastDateCut(lastDateCut)
                                            .build();
                                    collector.collect(loginInfoBean);
                                }

                                lastLoginData.update(loginDate);
                            }
                        }
                )
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWD_USER_LOGIN_INFO));



        //开窗无法修改回流用户的判定标准，因此不开窗。具体回流标准在查询语句中设置
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .reduce(
//                        new ReduceFunction<JSONObject>() {
//                            @Override
//                            public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
//                                //用户ID，登陆时间，登录时间戳，是否首次登陆，与上次登录日期差
//                                //窗口开始时间，结束时间，统计时间，首次登陆人数，1/7/30日回流人数
//                                value1.put("isNew", value1.getLongValue("isNew") + value2.getLongValue("isNew"));
//                                value1.put("1DayBack", (value1.getLongValue("lastDateCut") > 1 ? 1 : 0) + (value2.getLongValue("lastDateCut") > 1 ? 1 : 0));
//                                value1.put("7DayBack", (value1.getLongValue("lastDateCut") > 7 ? 1 : 0) + (value2.getLongValue("lastDateCut") > 7 ? 1 : 0));
//                                value1.put("30DayBack", (value1.getLongValue("lastDateCut") > 30 ? 1 : 0) + (value2.getLongValue("lastDateCut") > 30 ? 1 : 0));
//
//                                return value1;
//                            }
//                        },
//                        new ProcessAllWindowFunction<JSONObject, String, TimeWindow>() {
//                            @Override
//                            public void process(ProcessAllWindowFunction<JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<String> collector) throws Exception {
//                                JSONObject jsonObj=iterable.iterator().next();
//
//
//                            }
//                        }
//                )


        env.execute();


    }
}
