package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.educate.realtime.common.test.BeanToJsonStrMapFunction;
import com.atguigu.educate.realtime.common.util.DateFormatUtil;
import com.atguigu.educate.realtime.common.util.FlinkDorisUtil;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/13  10:12
 */


public class DwsTrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {
        //TODO 设置流处理环境 并行度 以及端口号
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10821, 4);

        //TODO 从kafka读取 数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("pageTag", "dws_traffic_is_new_page_view_window");
         //消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 对流中数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
//        jsonObjDS.print();
        //TODO 按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedDS
                = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 对分组后的数据进行处理  将结果再次进行转换
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        //~~~获取相关的维度~~~
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        String vc = commonJsonObj.getString("vc");
                        String ch = commonJsonObj.getString("ch");
                        String ar = commonJsonObj.getString("ar");
                        String isNew = commonJsonObj.getString("is_new");

                        //~~~获取相关的度量属性~~~
                        //独立访客计数
                        Long uvCt = 0L;
                        //从状态中获取当前设备上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }


                        //会话计数
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        if (pageJsonObj == null) {
                            return;
                        }
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                        //获取持续访问时长
                        Long duringTime = pageJsonObj.getLong("during_time");

                        TrafficPageViewBean viewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                svCt,
                                1L,
                                duringTime,
                                ts
                        );
                        out.collect(viewBean);
                    }
                }
        );
//        beanDS.print();
        //TODO 指定Watermark 的生成策略以及提取事件时间属性
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        //TODO 按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(
                                bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew()
                        );
                    }
                }
        );
        //TODO 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean pageViewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);
                        out.collect(pageViewBean);
                    }
                }
        );
        reduceDS.print();
        //TODO 将聚合的结果写到Doris表中
        reduceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkDorisUtil.getDorisSink("dws_traffic_is_new_page_view_window"));
        //TODO 提交作业
        env.execute();

    }
}
