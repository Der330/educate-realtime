package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsExaminationCourseTestBean;
import com.atguigu.educate.realtime.common.bean.DwsInteractionCourseReviewBean;
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

public class DwsExaminationCourseTestWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10101, 4);
        SingleOutputStreamOperator<DwsExaminationCourseTestBean> processDS = env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION, Constant.DWS_EXAMINATION_COURSE_TEST_WINDOW)
                        , WatermarkStrategy.noWatermarks(), Constant.DWS_EXAMINATION_COURSE_TEST_WINDOW)
                .process(new ProcessFunction<String, DwsExaminationCourseTestBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsExaminationCourseTestBean>.Context context, Collector<DwsExaminationCourseTestBean> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(DwsExaminationCourseTestBean.builder()
                                    .userId(jsonObj.getString("user_id"))
                                    .paperId(jsonObj.getString("paper_id"))
                                    .scoreSum(jsonObj.getBigDecimal("exam_score"))
                                    .durationSum(jsonObj.getLong("exam_duration_sec"))
                                    .ts(jsonObj.getLong("ts") * 1000)
                                    .UserCt(0L)
                                    .build());
                        }
                    }
                });
        SingleOutputStreamOperator<DwsExaminationCourseTestBean> reduceDS = AsyncDataStream.unorderedWait(processDS, new DimAsyncFunction<DwsExaminationCourseTestBean>() {
                    @Override
                    public void addDims(DwsExaminationCourseTestBean dwsExaminationCourseTestBean, JSONObject jsonObj) {
                        dwsExaminationCourseTestBean.setCourseId(jsonObj.getString("course_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_paper";
                    }

                    @Override
                    public String getRowKey(DwsExaminationCourseTestBean dwsExaminationCourseTestBean) {
                        return dwsExaminationCourseTestBean.getPaperId();
                    }
                }, 60, TimeUnit.SECONDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsExaminationCourseTestBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationCourseTestBean>() {
                            @Override
                            public long extractTimestamp(DwsExaminationCourseTestBean dwsExaminationCourseTestBean, long l) {
                                return dwsExaminationCourseTestBean.getTs();
                            }
                        }))
                .keyBy(bean -> bean.getUserId() + bean.getCourseId())
                .process(new KeyedProcessFunction<String, DwsExaminationCourseTestBean, DwsExaminationCourseTestBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsExaminationCourseTestBean dwsExaminationCourseTestBean, KeyedProcessFunction<String, DwsExaminationCourseTestBean, DwsExaminationCourseTestBean>.Context context, Collector<DwsExaminationCourseTestBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsExaminationCourseTestBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsExaminationCourseTestBean.setUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsExaminationCourseTestBean);
                    }
                })
                .keyBy(DwsExaminationCourseTestBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsExaminationCourseTestBean>() {
                    @Override
                    public DwsExaminationCourseTestBean reduce(DwsExaminationCourseTestBean dwsExaminationCourseTestBean, DwsExaminationCourseTestBean t1) throws Exception {
                        dwsExaminationCourseTestBean.setUserCt(dwsExaminationCourseTestBean.getUserCt() + t1.getUserCt());
                        dwsExaminationCourseTestBean.setScoreSum(dwsExaminationCourseTestBean.getScoreSum().add(t1.getScoreSum()));
                        dwsExaminationCourseTestBean.setDurationSum(dwsExaminationCourseTestBean.getDurationSum() + t1.getDurationSum());
                        return dwsExaminationCourseTestBean;
                    }
                }, new WindowFunction<DwsExaminationCourseTestBean, DwsExaminationCourseTestBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsExaminationCourseTestBean> iterable, Collector<DwsExaminationCourseTestBean> collector) throws Exception {
                        DwsExaminationCourseTestBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                });
        reduceDS.print();
        AsyncDataStream.unorderedWait(reduceDS,
                        new DimAsyncFunction<DwsExaminationCourseTestBean>() {
                            @Override
                            public void addDims(DwsExaminationCourseTestBean dwsExaminationCourseTestBean, JSONObject jsonObj) {
                                dwsExaminationCourseTestBean.setCourseName(jsonObj.getString("course_name"));
                            }

                            @Override
                            public String getTableName() {
                                return "dim_course_info";
                            }

                            @Override
                            public String getRowKey(DwsExaminationCourseTestBean dwsExaminationCourseTestBean) {
                                return dwsExaminationCourseTestBean.getCourseId();
                            }
                        }, 60, TimeUnit.SECONDS)
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_EXAMINATION_COURSE_TEST_WINDOW));
        env.execute();
    }
}
