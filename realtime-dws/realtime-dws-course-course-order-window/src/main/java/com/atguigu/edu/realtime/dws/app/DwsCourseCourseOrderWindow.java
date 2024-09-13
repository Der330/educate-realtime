package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsCourseCourseOrderBean;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.function.DimAsyncFunction;
import com.atguigu.educate.realtime.common.test.BeanToJsonStrMapFunction;
import com.atguigu.educate.realtime.common.util.DateFormatUtil;
import com.atguigu.educate.realtime.common.util.FlinkDorisUtil;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class DwsCourseCourseOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10101, 4);
        SingleOutputStreamOperator<DwsCourseCourseOrderBean> processDS = env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.DWS_COURSE_COURSE_ORDER_WINDOW)
                        , WatermarkStrategy.noWatermarks(), Constant.DWS_COURSE_COURSE_ORDER_WINDOW)
                .process(new ProcessFunction<String, DwsCourseCourseOrderBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsCourseCourseOrderBean>.Context context, Collector<DwsCourseCourseOrderBean> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(DwsCourseCourseOrderBean.builder()
                                    .userId(jsonObj.getString("user_id"))
                                    .courseId(jsonObj.getString("course_id"))
                                    .courseName(jsonObj.getString("course_name"))
                                    .orderCt(1L)
                                    .orderAmount(jsonObj.getBigDecimal("final_amount"))
                                    .ts(jsonObj.getLong("ts") * 1000)
                                    .build());
                        }
                    }
                });

        SingleOutputStreamOperator<DwsCourseCourseOrderBean> withCourseInfoDS = AsyncDataStream.unorderedWait(processDS, new DimAsyncFunction<DwsCourseCourseOrderBean>() {
            @Override
            public void addDims(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, JSONObject jsonObj) {
                dwsCourseCourseOrderBean.setSubjectId(jsonObj.getString("subject_id"));
            }

            @Override
            public String getTableName() {
                return "dim_course_info";
            }

            @Override
            public String getRowKey(DwsCourseCourseOrderBean dwsCourseCourseOrderBean) {
                return dwsCourseCourseOrderBean.getCourseId();
            }
        }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<DwsCourseCourseOrderBean> withSubjectInfoDS = AsyncDataStream.unorderedWait(withCourseInfoDS, new DimAsyncFunction<DwsCourseCourseOrderBean>() {
            @Override
            public void addDims(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, JSONObject jsonObj) {
                dwsCourseCourseOrderBean.setSubjectName(jsonObj.getString("subject_name"));
                dwsCourseCourseOrderBean.setCategoryId(jsonObj.getString("category_id"));
            }

            @Override
            public String getTableName() {
                return "dim_base_subject_info";
            }

            @Override
            public String getRowKey(DwsCourseCourseOrderBean dwsCourseCourseOrderBean) {
                return dwsCourseCourseOrderBean.getSubjectId();
            }
        }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<DwsCourseCourseOrderBean> reduceDS = withSubjectInfoDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsCourseCourseOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsCourseCourseOrderBean>() {
                            @Override
                            public long extractTimestamp(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, long l) {
                                return dwsCourseCourseOrderBean.getTs();
                            }
                        }))
                .keyBy(bean -> bean.getUserId() + bean.getCourseId())
                .process(new KeyedProcessFunction<String, DwsCourseCourseOrderBean, DwsCourseCourseOrderBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, KeyedProcessFunction<String, DwsCourseCourseOrderBean, DwsCourseCourseOrderBean>.Context context, Collector<DwsCourseCourseOrderBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsCourseCourseOrderBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsCourseCourseOrderBean.setCourseUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsCourseCourseOrderBean);
                    }
                })
                .keyBy(bean -> bean.getUserId() + bean.getSubjectId())
                .process(new KeyedProcessFunction<String, DwsCourseCourseOrderBean, DwsCourseCourseOrderBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, KeyedProcessFunction<String, DwsCourseCourseOrderBean, DwsCourseCourseOrderBean>.Context context, Collector<DwsCourseCourseOrderBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsCourseCourseOrderBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsCourseCourseOrderBean.setSubjectUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsCourseCourseOrderBean);
                    }
                })
                .keyBy(bean -> bean.getUserId() + bean.getCategoryId())
                .process(new KeyedProcessFunction<String, DwsCourseCourseOrderBean, DwsCourseCourseOrderBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, KeyedProcessFunction<String, DwsCourseCourseOrderBean, DwsCourseCourseOrderBean>.Context context, Collector<DwsCourseCourseOrderBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsCourseCourseOrderBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsCourseCourseOrderBean.setCategoryUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsCourseCourseOrderBean);
                    }
                })
                .keyBy(DwsCourseCourseOrderBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsCourseCourseOrderBean>() {
                    @Override
                    public DwsCourseCourseOrderBean reduce(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, DwsCourseCourseOrderBean t1) throws Exception {
                        dwsCourseCourseOrderBean.setOrderCt(dwsCourseCourseOrderBean.getOrderCt() + t1.getOrderCt());
                        dwsCourseCourseOrderBean.setOrderAmount(dwsCourseCourseOrderBean.getOrderAmount().add(t1.getOrderAmount()));
                        dwsCourseCourseOrderBean.setCourseUserCt(dwsCourseCourseOrderBean.getCourseUserCt() + t1.getCourseUserCt());
                        dwsCourseCourseOrderBean.setSubjectUserCt(dwsCourseCourseOrderBean.getSubjectUserCt() + t1.getSubjectUserCt());
                        dwsCourseCourseOrderBean.setCategoryUserCt(dwsCourseCourseOrderBean.getCategoryUserCt() + t1.getCategoryUserCt());
                        return dwsCourseCourseOrderBean;
                    }
                }, new WindowFunction<DwsCourseCourseOrderBean, DwsCourseCourseOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsCourseCourseOrderBean> iterable, Collector<DwsCourseCourseOrderBean> collector) throws Exception {
                        DwsCourseCourseOrderBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                });
        reduceDS.print();
        AsyncDataStream.unorderedWait(reduceDS,
                        new DimAsyncFunction<DwsCourseCourseOrderBean>() {
                            @Override
                            public void addDims(DwsCourseCourseOrderBean dwsCourseCourseOrderBean, JSONObject jsonObj) {
                                dwsCourseCourseOrderBean.setCategoryName(jsonObj.getString("category_name"));
                            }

                            @Override
                            public String getTableName() {
                                return "dim_base_category_info";
                            }

                            @Override
                            public String getRowKey(DwsCourseCourseOrderBean dwsCourseCourseOrderBean) {
                                return dwsCourseCourseOrderBean.getCategoryId();
                            }
                        }, 60, TimeUnit.SECONDS)
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_COURSE_COURSE_ORDER_WINDOW));
        env.execute();
    }
}
