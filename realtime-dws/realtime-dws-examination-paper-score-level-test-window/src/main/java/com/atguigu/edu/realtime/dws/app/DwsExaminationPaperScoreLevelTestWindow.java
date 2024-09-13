package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsExaminationPaperScoreLevelTestBean;
import com.atguigu.educate.realtime.common.bean.DwsExaminationPaperTestBean;
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

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

public class DwsExaminationPaperScoreLevelTestWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10101, 4);
        SingleOutputStreamOperator<DwsExaminationPaperScoreLevelTestBean> reduceDS = env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION, Constant.DWS_EXAMINATION_PAPER_SCORE_LEVEL_TEST_WINDOW)
                        , WatermarkStrategy.noWatermarks(), Constant.DWS_EXAMINATION_PAPER_SCORE_LEVEL_TEST_WINDOW)
                .process(new ProcessFunction<String, DwsExaminationPaperScoreLevelTestBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsExaminationPaperScoreLevelTestBean>.Context context, Collector<DwsExaminationPaperScoreLevelTestBean> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(DwsExaminationPaperScoreLevelTestBean.builder()
                                    .userId(jsonObj.getString("user_id"))
                                    .paperId(jsonObj.getString("paper_id"))
                                    .scoreLevel(jsonObj.getBigDecimal("exam_score").compareTo(BigDecimal.valueOf(60)) >= 0 ? "及格" : "不及格")
                                    .ts(jsonObj.getLong("ts") * 1000)
                                    .UserCt(0L)
                                    .build());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsExaminationPaperScoreLevelTestBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationPaperScoreLevelTestBean>() {
                            @Override
                            public long extractTimestamp(DwsExaminationPaperScoreLevelTestBean dwsExaminationPaperScoreLevelTestBean, long l) {
                                return dwsExaminationPaperScoreLevelTestBean.getTs();
                            }
                        }))
                .keyBy(bean -> bean.getUserId() + bean.getPaperId() + bean.getScoreLevel())
                .process(new KeyedProcessFunction<String, DwsExaminationPaperScoreLevelTestBean, DwsExaminationPaperScoreLevelTestBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsExaminationPaperScoreLevelTestBean dwsExaminationPaperScoreLevelTestBean, KeyedProcessFunction<String, DwsExaminationPaperScoreLevelTestBean, DwsExaminationPaperScoreLevelTestBean>.Context context, Collector<DwsExaminationPaperScoreLevelTestBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsExaminationPaperScoreLevelTestBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsExaminationPaperScoreLevelTestBean.setUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsExaminationPaperScoreLevelTestBean);
                    }
                })
                .keyBy(bean -> bean.getPaperId() + bean.getScoreLevel())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsExaminationPaperScoreLevelTestBean>() {
                    @Override
                    public DwsExaminationPaperScoreLevelTestBean reduce(DwsExaminationPaperScoreLevelTestBean dwsExaminationPaperScoreLevelTestBean, DwsExaminationPaperScoreLevelTestBean t1) throws Exception {
                        dwsExaminationPaperScoreLevelTestBean.setUserCt(dwsExaminationPaperScoreLevelTestBean.getUserCt() + t1.getUserCt());
                        return dwsExaminationPaperScoreLevelTestBean;
                    }
                }, new WindowFunction<DwsExaminationPaperScoreLevelTestBean, DwsExaminationPaperScoreLevelTestBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsExaminationPaperScoreLevelTestBean> iterable, Collector<DwsExaminationPaperScoreLevelTestBean> collector) throws Exception {
                        DwsExaminationPaperScoreLevelTestBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                });
        AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<DwsExaminationPaperScoreLevelTestBean>() {
                    @Override
                    public void addDims(DwsExaminationPaperScoreLevelTestBean dwsExaminationPaperTestBean, JSONObject jsonObj) {
                        dwsExaminationPaperTestBean.setPaperTitle(jsonObj.getString("paper_title"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_paper";
                    }

                    @Override
                    public String getRowKey(DwsExaminationPaperScoreLevelTestBean dwsExaminationPaperTestBean) {
                        return dwsExaminationPaperTestBean.getPaperId();
                    }
                }, 60, TimeUnit.SECONDS)
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_EXAMINATION_PAPER_SCORE_LEVEL_TEST_WINDOW));
        env.execute();
    }
}
