package com.atguigu.educat.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class DimApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10002, 4);

        DataStreamSource<String> sourceDS=env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.TOPIC_DB,"DimApp"), WatermarkStrategy.noWatermarks(), "DimApp");

        //转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS=sourceDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> filterDS=jsonObjDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                String database=jsonObject.getString("database");
                String data=jsonObject.getString("data");

                if ("edu".equals(database) && data.length() > 2) {
                    collector.collect(jsonObject);
                }

            }
        });


        //获得维度配置信息表
        Properties properties=new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource=MySqlSource.<String>builder()
                .hostname("localhost")
                .port(13306)
                .jdbcProperties(properties)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("edu_config")
                .tableList("edu_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSourceDS=env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlCDCSource");




        env.execute();

        //




    }
}
