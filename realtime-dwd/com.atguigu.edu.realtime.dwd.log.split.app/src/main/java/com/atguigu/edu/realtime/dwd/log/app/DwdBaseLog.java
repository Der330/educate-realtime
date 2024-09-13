package com.atguigu.edu.realtime.dwd.log.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.DateFormatUtil;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import com.atguigu.educate.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/12  10:31
 */


public class DwdBaseLog {

    private final String ERR = "err";
    private final String START = "start";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private static final String PAGE = "page";


    public static void main(String[] args) throws Exception {
        //TODO 设置流处理环境 以及端口号和并行度
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(18001, 4);
        //TODO 从kafka主题读取数据
        KafkaSource<String> kafkaSource = FlinkKafkaUtil.getKafkaSource("topic_log", "dwd_base_log");
        //TODO 封装为流
        DataStreamSource<String> kafkaStrDS =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //TODO 对流中数据类型进行转换  jsonStr->jsonObj
        //定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        //转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            //如果转换的过程中 没有发生异常，说明是标准的json，继续向下游传递
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            //如果转换的过程中 发生了异常，说明不是标准的json，属于脏数据，放到侧输出流中
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
//        jsonObjDS.print("标准");
//        env.execute();
        //脏数据
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        //将测输出流的脏数据发送到kafka主题
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        //TODO 新老访客标记的修复
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
//        keyedDS.print("keyedDS");
        //使用flink的状态编程进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    //声明状态
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            //如果is_new的值为1，则判断是否是第一次访问，如果第一次，则标记为0，并且将时间写入状态，如果已经访问过，则直接输出
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }

                        } else {
                            //如果 is_new 的值为 0
                            //如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。
                            // 当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，
                            // 只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                long yesterDayTs = ts - 24 * 60 * 60 * 1000;
                                String yesterDay = DateFormatUtil.tsToDate(yesterDayTs);
                                lastVisitDateState.update(yesterDay);
                            }

                        }
                        collector.collect(jsonObject);
                    }
                }
        );
        fixedDS.print("fixedDS");
        //TODO 分流 将不同类型的日志放到不同的流
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> appVideo = new OutputTag<String>("appVideo") {
        };

        SingleOutputStreamOperator<String> LogDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //~~~错误日志~~~
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            //将错误日志放到错误侧输出流
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            //~~~启动日志~~~
                            //将启动日志放到启动侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //~~~页面日志~~~
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");


                            //~~~曝光日志~~~
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                //遍历出所有曝光数据
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    //定义一个新的json，用于封装当前遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    //将曝光数据写到曝光侧输出流中
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            //~~~动作日志~~~
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历出当前页面上的所有动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    //将动作数据写到动作侧输出流中
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            //~~~视频播放日志~~
                            JSONObject appVideo2 = jsonObj.getJSONObject("appVideo");
                            if (appVideo2 != null) {
                                //遍历出所有曝光数据
                                //定义一个新的json，用于封装当前遍历出来的曝光数据
                                JSONObject newDisplayJsonObj = new JSONObject();
                                newDisplayJsonObj.put("common", commonJsonObj);
                                newDisplayJsonObj.put("page", pageJsonObj);
                                newDisplayJsonObj.put("ts", ts);
                                newDisplayJsonObj.put("appVideo", appVideo2);
                                //将曝光数据写到曝光侧输出流中
                                ctx.output(appVideo, newDisplayJsonObj.toJSONString());

                                jsonObj.remove("appVideo");
                            }

                            //~~~页面日志~~

                            //将页面日志发送到主流
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                });


        // TODO 将不同流的数据写到kafka主题
        SideOutputDataStream<String> startLog = LogDS.getSideOutput(startTag);
        startLog.sinkTo(FlinkSinkUtil.getKafkaSink("startTag"));
        SideOutputDataStream<String> errTagLog = LogDS.getSideOutput(errTag);
        errTagLog.sinkTo(FlinkSinkUtil.getKafkaSink("errTag"));
        SideOutputDataStream<String> displayTagLog = LogDS.getSideOutput(displayTag);
        displayTagLog.sinkTo(FlinkSinkUtil.getKafkaSink("displayTag"));
        SideOutputDataStream<String> actionTagLog = LogDS.getSideOutput(actionTag);
        actionTagLog.sinkTo(FlinkSinkUtil.getKafkaSink("actionTag"));
        SideOutputDataStream<String> appVideoLog = LogDS.getSideOutput(appVideo);
        appVideoLog.sinkTo(FlinkSinkUtil.getKafkaSink("appVideo"));
        LogDS.sinkTo(FlinkSinkUtil.getKafkaSink("pageTag"));
        // TODO 提交作业
        env.execute();
    }


}



