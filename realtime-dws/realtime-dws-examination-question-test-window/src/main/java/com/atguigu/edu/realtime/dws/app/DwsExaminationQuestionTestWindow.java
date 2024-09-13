package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsExaminationQuestionTestBean;
import com.atguigu.educate.realtime.common.constant.Constant;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DwsExaminationQuestionTestWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10101, 4);
        env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION, Constant.DWS_EXAMINATION_PAPER_TEST_WINDOW)
                        , WatermarkStrategy.noWatermarks(), Constant.DWS_EXAMINATION_PAPER_TEST_WINDOW)
                .process(new ProcessFunction<String, DwsExaminationQuestionTestBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsExaminationQuestionTestBean>.Context context, Collector<DwsExaminationQuestionTestBean> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(DwsExaminationQuestionTestBean.builder()
                                    .userId(jsonObj.getString("user_id"))
                                    .questionId(jsonObj.getString("question_id"))
                                    .answerCt(1L)
                                    .correctAnswerCt(jsonObj.getLong("is_correct"))
                                    .answerUserCt(0L)
                                    .correctAnswerUserCt(0L)
                                    .ts(jsonObj.getLong("ts") * 1000)
                                    .build());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsExaminationQuestionTestBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationQuestionTestBean>() {
                            @Override
                            public long extractTimestamp(DwsExaminationQuestionTestBean dwsExaminationQuestionTestBean, long l) {
                                return dwsExaminationQuestionTestBean.getTs();
                            }
                        }))
                .keyBy(DwsExaminationQuestionTestBean::getUserId)
                .process(new KeyedProcessFunction<String, DwsExaminationQuestionTestBean, DwsExaminationQuestionTestBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsExaminationQuestionTestBean dwsExaminationQuestionTestBean, KeyedProcessFunction<String, DwsExaminationQuestionTestBean, DwsExaminationQuestionTestBean>.Context context, Collector<DwsExaminationQuestionTestBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsExaminationQuestionTestBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsExaminationQuestionTestBean.setUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsExaminationQuestionTestBean);
                    }
                })
                .keyBy(DwsExaminationQuestionTestBean::getQuestionId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsExaminationQuestionTestBean>() {
                    @Override
                    public DwsExaminationQuestionTestBean reduce(DwsExaminationQuestionTestBean dwsExaminationQuestionTestBean, DwsExaminationQuestionTestBean t1) throws Exception {
                        dwsExaminationQuestionTestBean.setUserCt(dwsExaminationQuestionTestBean.getUserCt() + t1.getUserCt());
                        dwsExaminationQuestionTestBean.setScoreSum(dwsExaminationQuestionTestBean.getScoreSum().add(t1.getScoreSum()));
                        dwsExaminationQuestionTestBean.setDurationSum(dwsExaminationQuestionTestBean.getDurationSum() + t1.getDurationSum());
                        return dwsExaminationQuestionTestBean;
                    }
                }, new WindowFunction<DwsExaminationQuestionTestBean, DwsExaminationQuestionTestBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsExaminationQuestionTestBean> iterable, Collector<DwsExaminationQuestionTestBean> collector) throws Exception {
                        DwsExaminationQuestionTestBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                })
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_EXAMINATION_PAPER_TEST_WINDOW));
        env.execute();
    }
}
