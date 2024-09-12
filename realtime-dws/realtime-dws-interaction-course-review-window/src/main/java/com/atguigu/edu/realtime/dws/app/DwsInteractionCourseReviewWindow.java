package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class DwsInteractionCourseReviewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10100, 4);
        SingleOutputStreamOperator<DwsInteractionCourseReviewBean> reduceDS = env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.DWD_INTERACTION_REVIEW_INFO, Constant.DWS_INTERACTION_COURSE_REVIEW_WINDOW)
                        , WatermarkStrategy.noWatermarks(), Constant.DWS_INTERACTION_COURSE_REVIEW_WINDOW)
                .process(new ProcessFunction<String, DwsInteractionCourseReviewBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsInteractionCourseReviewBean>.Context context, Collector<DwsInteractionCourseReviewBean> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(DwsInteractionCourseReviewBean.builder()
                                    .courseId(jsonObj.getString("course_id"))
                                    .reviewScoreSum(jsonObj.getLong("review_stars"))
                                    .reviewUserCt(1L)
                                    .goodReviewUserCt(jsonObj.getLong("review_stars") > 3 ? 1L : 0L)
                                    .ts(jsonObj.getLong("ts") * 1000)
                                    .build());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsInteractionCourseReviewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsInteractionCourseReviewBean>() {
                            @Override
                            public long extractTimestamp(DwsInteractionCourseReviewBean dwsInteractionCourseReviewBean, long l) {
                                return dwsInteractionCourseReviewBean.getTs();
                            }
                        }))
                .keyBy(DwsInteractionCourseReviewBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsInteractionCourseReviewBean>() {
                    @Override
                    public DwsInteractionCourseReviewBean reduce(DwsInteractionCourseReviewBean value1, DwsInteractionCourseReviewBean value2) throws Exception {
                        value1.setReviewUserCt(value1.getReviewUserCt() + value2.getReviewUserCt());
                        value1.setGoodReviewUserCt(value1.getGoodReviewUserCt() + value2.getGoodReviewUserCt());
                        value1.setReviewScoreSum(value1.getReviewScoreSum() + value2.getReviewScoreSum());
                        return value1;
                    }
                }, new WindowFunction<DwsInteractionCourseReviewBean, DwsInteractionCourseReviewBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsInteractionCourseReviewBean> iterable, Collector<DwsInteractionCourseReviewBean> collector) throws Exception {
                        DwsInteractionCourseReviewBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                });
        AsyncDataStream.unorderedWait(reduceDS,
                        new DimAsyncFunction<DwsInteractionCourseReviewBean>() {
                            @Override
                            public void addDims(DwsInteractionCourseReviewBean dwsInteractionCourseReviewBean, JSONObject jsonObj) {
                                dwsInteractionCourseReviewBean.setCourseName(jsonObj.getString("course_name"));
                            }

                            @Override
                            public String getTableName() {
                                return "dim-course_info";
                            }

                            @Override
                            public String getRowKey(DwsInteractionCourseReviewBean dwsInteractionCourseReviewBean) {
                                return dwsInteractionCourseReviewBean.getCourseId();
                            }
                        }, 60, TimeUnit.SECONDS)
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_INTERACTION_COURSE_REVIEW_WINDOW));
        env.execute();
    }
}
