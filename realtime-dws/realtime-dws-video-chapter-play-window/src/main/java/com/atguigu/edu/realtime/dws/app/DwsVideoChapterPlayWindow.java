package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.DwsVideoChapterPlayWindowBean;
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

public class DwsVideoChapterPlayWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10101, 4);
        SingleOutputStreamOperator<DwsVideoChapterPlayWindowBean> processDS = env.fromSource(FlinkKafkaUtil.getKafkaSource("appVideo", Constant.DWS_VIDEO_CHAPTER_PLAY_WINDOW)
                        , WatermarkStrategy.noWatermarks(), Constant.DWS_VIDEO_CHAPTER_PLAY_WINDOW)
                .process(new ProcessFunction<String, DwsVideoChapterPlayWindowBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsVideoChapterPlayWindowBean>.Context context, Collector<DwsVideoChapterPlayWindowBean> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(DwsVideoChapterPlayWindowBean.builder()
                                    .videoId(jsonObj.getJSONObject("appVideo").getString("video_id"))
                                    .mid(jsonObj.getJSONObject("common").getString("mid"))
                                    .playCt(jsonObj.getJSONObject("appVideo").getLong("play_sec") < 30 ? 1L : 0L)
                                    .playDuration(jsonObj.getJSONObject("appVideo").getLong("play_sec"))
                                    .ts(jsonObj.getLong("ts") * 1000)
                                    .playUserCt(0L)
                                    .build());
                        }
                    }
                });
        SingleOutputStreamOperator<DwsVideoChapterPlayWindowBean> reduceDS = AsyncDataStream.unorderedWait(processDS, new DimAsyncFunction<DwsVideoChapterPlayWindowBean>() {
                    @Override
                    public void addDims(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean, JSONObject jsonObj) {
                        dwsVideoChapterPlayWindowBean.setChapterId(jsonObj.getString("chapter_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_video_info";
                    }

                    @Override
                    public String getRowKey(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean) {
                        return dwsVideoChapterPlayWindowBean.getVideoId();
                    }
                }, 60, TimeUnit.SECONDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsVideoChapterPlayWindowBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsVideoChapterPlayWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean, long l) {
                                return dwsVideoChapterPlayWindowBean.getTs();
                            }
                        }))
                .keyBy(bean -> bean.getMid() + bean.getChapterId())
                .process(new KeyedProcessFunction<String, DwsVideoChapterPlayWindowBean, DwsVideoChapterPlayWindowBean>() {
                    ValueState<String> lastDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastDate = new ValueStateDescriptor<>("lastDate", String.class);
                        lastDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(lastDate);
                    }

                    @Override
                    public void processElement(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean, KeyedProcessFunction<String, DwsVideoChapterPlayWindowBean, DwsVideoChapterPlayWindowBean>.Context context, Collector<DwsVideoChapterPlayWindowBean> collector) throws Exception {
                        String lastDate = lastDateState.value();
                        String todayDate = DateFormatUtil.tsToDate(dwsVideoChapterPlayWindowBean.getTs());
                        if (StringUtils.isEmpty(lastDate) || !lastDate.equals(todayDate)) {
                            dwsVideoChapterPlayWindowBean.setPlayUserCt(1L);
                            lastDateState.update(todayDate);
                        }
                        collector.collect(dwsVideoChapterPlayWindowBean);
                    }
                })
                .keyBy(DwsVideoChapterPlayWindowBean::getChapterId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsVideoChapterPlayWindowBean>() {
                    @Override
                    public DwsVideoChapterPlayWindowBean reduce(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean, DwsVideoChapterPlayWindowBean t1) throws Exception {
                        dwsVideoChapterPlayWindowBean.setPlayCt(dwsVideoChapterPlayWindowBean.getPlayCt() + t1.getPlayCt());
                        dwsVideoChapterPlayWindowBean.setPlayDuration(dwsVideoChapterPlayWindowBean.getPlayDuration() + t1.getPlayDuration());
                        dwsVideoChapterPlayWindowBean.setPlayUserCt(dwsVideoChapterPlayWindowBean.getPlayUserCt() + t1.getPlayUserCt());
                        return dwsVideoChapterPlayWindowBean;
                    }
                }, new WindowFunction<DwsVideoChapterPlayWindowBean, DwsVideoChapterPlayWindowBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsVideoChapterPlayWindowBean> iterable, Collector<DwsVideoChapterPlayWindowBean> collector) throws Exception {
                        DwsVideoChapterPlayWindowBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));
                        collector.collect(bean);
                    }
                });
        reduceDS.print();
        AsyncDataStream.unorderedWait(reduceDS,
                        new DimAsyncFunction<DwsVideoChapterPlayWindowBean>() {
                            @Override
                            public void addDims(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean, JSONObject jsonObj) {
                                dwsVideoChapterPlayWindowBean.setChapterName(jsonObj.getString("chapter_name"));
                            }

                            @Override
                            public String getTableName() {
                                return "chapter_info";
                            }

                            @Override
                            public String getRowKey(DwsVideoChapterPlayWindowBean dwsVideoChapterPlayWindowBean) {
                                return dwsVideoChapterPlayWindowBean.getChapterId();
                            }
                        }, 60, TimeUnit.SECONDS)
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink(Constant.DWS_VIDEO_CHAPTER_PLAY_WINDOW));
        env.execute();
    }
}
