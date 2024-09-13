package com.atguigu.eaducat.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsUserPageBean;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.function.DwsUserPageProcessFunction;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserPageWindows {
    public static void main(String[] args) {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10035, 4);

        //主页、商品详情
        SingleOutputStreamOperator<DwsUserPageBean> homeDetailDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_PAGE_TAG, "DwsUserPageWindows"), WatermarkStrategy.noWatermarks(), "pageLog")
                .map(JSONObject::parseObject)
                .filter(jsonObj -> jsonObj.getJSONObject("page").getString("page_id").equals("home")
                        || jsonObj.getJSONObject("page").getString("page_id").equals("course_detail")
                )
                .keyBy(jsonObj -> Tuple2.of(jsonObj.getString("page_id"),jsonObj.getString("uid")))
                .process(
                        new DwsUserPageProcessFunction<Tuple2<String, String>>() {
                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<Tuple2<String, String>, JSONObject, DwsUserPageBean>.Context context, Collector<DwsUserPageBean> collector) throws Exception {
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
                                    userPageBean.setTs(ts);
                                    userPageBean.setAddCarCount(1L);

                                    collector.collect(userPageBean);
                                    state.update("1");
                                }
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsUserPageBean>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<DwsUserPageBean>() {
                                    @Override
                                    public long extractTimestamp(DwsUserPageBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
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

                                    userPageBean.setTs(ts);
                                    userPageBean.setPaySucCount(1L);
                                    state.update("1");
                                }
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwsUserPageBean>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<DwsUserPageBean>() {
                                            @Override
                                            public long extractTimestamp(DwsUserPageBean element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );


        //
    }
}
