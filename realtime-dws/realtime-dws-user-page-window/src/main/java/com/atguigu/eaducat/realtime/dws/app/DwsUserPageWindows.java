package com.atguigu.eaducat.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsUserPageBean;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.function.DwsUserPageProcessFunction;
import com.atguigu.educate.realtime.common.test.BeanToJsonStrMapFunction;
import com.atguigu.educate.realtime.common.util.DateFormatUtil;
import com.atguigu.educate.realtime.common.util.FlinkDorisUtil;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserPageWindows {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10035, 4);


        //主页、商品详情
        SingleOutputStreamOperator<DwsUserPageBean> homeDetailDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_PAGE_TAG, "DwsUserPageWindows"), WatermarkStrategy.noWatermarks(), "pageLog")
                .map(JSONObject::parseObject)
                .filter(jsonObj -> jsonObj.getJSONObject("page") != null
                                && (    jsonObj.getJSONObject("page").getString("page_id").equals("home")
                                || jsonObj.getJSONObject("page").getString("page_id").equals("course_detail")
                        )
                )
                .keyBy(jsonObj -> jsonObj.getJSONObject("page").getString("page_id")+jsonObj.getJSONObject("common").getString("uid"))
                .process(
                        new DwsUserPageProcessFunction<String>() {
                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsUserPageBean>.Context context, Collector<DwsUserPageBean> collector) throws Exception {
                                if (state.value()== null) {
                                    DwsUserPageBean userPageBean=DwsUserPageBean.builder().build();
                                    userPageBean.initBean();
                                    String pageID=jsonObject.getJSONObject("page").getString("page_id");
                                    Long ts=jsonObject.getLong("ts");
                                    if ("home".equals(pageID)) {
                                        userPageBean.setHomeViewCount(1L);
                                    } else {
                                        userPageBean.setDetailViewCount(1L);
                                    }
                                    userPageBean.setTs(ts);

                                    collector.collect(userPageBean);
                                    state.update("1");
                                }
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsUserPageBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<DwsUserPageBean>() {
                                            @Override
                                            public long extractTimestamp(DwsUserPageBean element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );



        //加购
        SingleOutputStreamOperator<DwsUserPageBean> addCartDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_CART_ADD, "DwsUserPageWindows"), WatermarkStrategy.noWatermarks(), Constant.TOPIC_DWD_TRADE_CART_ADD)
                .map(JSONObject::parseObject)
                .keyBy(jsonObj -> jsonObj.getString("user_id"))
                .process(
                        new DwsUserPageProcessFunction<String>() {
                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsUserPageBean>.Context context, Collector<DwsUserPageBean> collector) throws Exception {
                                if (state.value() == null) {
                                    DwsUserPageBean userPageBean=DwsUserPageBean.builder().build();
                                    userPageBean.initBean();

                                    Long ts=jsonObject.getLong("ts");
                                    userPageBean.setTs(ts*1000);
                                    userPageBean.setAddCarCount(1L);

                                    collector.collect(userPageBean);
                                    state.update("1");
                                }
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsUserPageBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<DwsUserPageBean>() {
                                    @Override
                                    public long extractTimestamp(DwsUserPageBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        //订单
        SingleOutputStreamOperator<DwsUserPageBean> orderInfoDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_INFO, "DwsUserPageWindows"), WatermarkStrategy.noWatermarks(), "orderInfoSource")
                .map(JSONObject::parseObject)
                .keyBy(jsonObj -> jsonObj.getString("user_id"))
                .process(
                        new DwsUserPageProcessFunction<String>() {
                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsUserPageBean>.Context context, Collector<DwsUserPageBean> collector) throws Exception {

                                if (state.value() == null){

                                    DwsUserPageBean userPageBean=DwsUserPageBean.builder().build();
                                    userPageBean.initBean();

                                    Long ts=jsonObject.getLong("ts");

                                    userPageBean.setDoOrderCount(1L);
                                    userPageBean.setTs(ts*1000);

                                    collector.collect(userPageBean);
                                    state.update("1");

                                }

                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsUserPageBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<DwsUserPageBean>() {
                                            @Override
                                            public long extractTimestamp(DwsUserPageBean element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );

        //支付成功
        SingleOutputStreamOperator<DwsUserPageBean> paySucDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS, "DwsUserPageWindows"), WatermarkStrategy.noWatermarks(), "paySucSource")
                .map(JSONObject::parseObject)
                .keyBy(jsonObj -> jsonObj.getString("user_id"))
                .process(
                        new DwsUserPageProcessFunction<String>() {
                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsUserPageBean>.Context context, Collector<DwsUserPageBean> collector) throws Exception {

                                if (state.value() == null) {
                                    DwsUserPageBean userPageBean=DwsUserPageBean.builder().build();
                                    userPageBean.initBean();
                                    Long ts=jsonObject.getLong("ts");

                                    userPageBean.setTs(ts*1000);
                                    userPageBean.setPaySucCount(1L);

                                    collector.collect(userPageBean);
                                    state.update("1");
                                }
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsUserPageBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<DwsUserPageBean>() {
                                            @Override
                                            public long extractTimestamp(DwsUserPageBean element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );



        homeDetailDS
                .union(addCartDS).union(orderInfoDS).union(paySucDS)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<DwsUserPageBean>() {
                            @Override
                            public DwsUserPageBean reduce(DwsUserPageBean value1, DwsUserPageBean value2) throws Exception {
                                return value1.add(value2);
                            }
                        },
                        new ProcessAllWindowFunction<DwsUserPageBean, DwsUserPageBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<DwsUserPageBean, DwsUserPageBean, TimeWindow>.Context context, Iterable<DwsUserPageBean> iterable, Collector<DwsUserPageBean> collector) throws Exception {

                                TimeWindow window=context.window();

                                long startTs=window.getStart();
                                long endTs=window.getEnd();

                                String stt=DateFormatUtil.tsToDateTime(startTs);
                                String edt=DateFormatUtil.tsToDateTime(endTs);
                                String curDate=DateFormatUtil.tsToDate(startTs);

                                DwsUserPageBean userPageBean=iterable.iterator().next();

                                userPageBean.setStt(stt);
                                userPageBean.setEdt(edt);
                                userPageBean.setCurDate(curDate);

                                collector.collect(userPageBean);
                            }
                        }
                )
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_USER_PAGE_WINDOW));

        env.execute();

    }
}
