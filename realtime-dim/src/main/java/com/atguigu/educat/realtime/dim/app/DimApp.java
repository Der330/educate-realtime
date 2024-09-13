package com.atguigu.educat.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.educate.realtime.common.bean.TableProcessDim;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import com.atguigu.educate.realtime.common.util.HbaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
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
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .jdbcProperties(properties)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("edu_config")
                .tableList("edu_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSourceDS=env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlCDCSource");


        SingleOutputStreamOperator<TableProcessDim> tableProcessDS=mysqlSourceDS.map(new RichMapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String value) throws Exception {
                JSONObject jsonObj=JSON.parseObject(value);
                JSONObject before=jsonObj.getJSONObject("before");
                JSONObject after=jsonObj.getJSONObject("after");

                String op=jsonObj.getString("op");

                //r u d i
                TableProcessDim tableProcessDim=null;
                if ("d".equals(op)) {
                    tableProcessDim=JSONObject.parseObject(before.toJSONString(), TableProcessDim.class);
                } else {
                    tableProcessDim=JSONObject.parseObject(after.toJSONString(), TableProcessDim.class);
                }
                tableProcessDim.setOp(op);

                return tableProcessDim;
            }
        })
        .map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection hbaseConnect;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConnect=HbaseUtil.getHbaseConnect();
                    }

                    @Override
                    public void close() throws Exception {
                        HbaseUtil.closeHbaseConnect(hbaseConnect);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {


                        String op=tableProcessDim.getOp();
                        String sinkTable=tableProcessDim.getSinkTable();
                        String[] colFamilys=tableProcessDim.getSinkFamily().split(",");
                        if ("c".equals(op) || "r".equals(op)) {
                            System.out.println("创建表");
                            HbaseUtil.createHbaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, sinkTable, colFamilys);
                        }
                        if ("d".equals(op)) {
                            HbaseUtil.dropHbaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, sinkTable);
                        }
                        if ("u".equals(op)) {
                            HbaseUtil.dropHbaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, sinkTable);
                            HbaseUtil.createHbaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, sinkTable, colFamilys);
                        }
                        return tableProcessDim;
                    }
                }
        );


        //创建广播流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor=new MapStateDescriptor<String, TableProcessDim>("dimProcessTable", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS=tableProcessDS.broadcast(mapStateDescriptor);


        //连接广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectDS=filterDS.connect(broadcastDS)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    Connection hbaseConn;

                    Map<String, TableProcessDim> dimTableProcessMap=new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn=HbaseUtil.getHbaseConnect();

                        //预加载Mysql的维度信息配置表，放在Map里
                        Class.forName(Constant.MYSQL_DRIVER);

                        java.sql.Connection connection=DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);

                        String sql="select * from edu_config.table_process_dim";

                        PreparedStatement ps=connection.prepareStatement(sql);

                        ResultSet resultSet=ps.executeQuery();
                        ResultSetMetaData metaData=resultSet.getMetaData();

                        while (resultSet.next()) {
                            JSONObject jsonObject=new JSONObject();
                            for (int i=1; i <= metaData.getColumnCount(); i++) {
                                String calName=metaData.getColumnName(i);
                                Object colValue=resultSet.getObject(i);
                                jsonObject.put(calName, colValue);
                            }
                            TableProcessDim tableProcessDim=JSONObject.parseObject(jsonObject.toJSONString(), TableProcessDim.class);
                            dimTableProcessMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }

                        System.out.println("维度表配置信息预加载完成");

                    }

                    @Override
                    public void close() throws Exception {
                        HbaseUtil.closeHbaseConnect(hbaseConn);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {

                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState=readOnlyContext.getBroadcastState(mapStateDescriptor);

                        String table=jsonObject.getString("table");

                        //检查主流中数据是否为维度表
                        TableProcessDim tableProcessDim;
                        if ((tableProcessDim=dimTableProcessMap.get(table)) != null || (tableProcessDim=broadcastState.get(table)) != null) {

                            JSONObject data=jsonObject.getJSONObject("data");
                            String type=jsonObject.getString("type");
                            String rowKay=data.getString(tableProcessDim.getSinkRowKey());


                            //判断维度表数据操作类型
                            if ("delete".equals(type)) {

                                HbaseUtil.deleteRow(hbaseConn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkRowKey());
                            } else {

                                String[] colNames=tableProcessDim.getSinkColumns().split(",");

                                //提取数据变化中的字段值
                                JSONObject colObj=new JSONObject();
                                for (String colName : colNames) {
                                    String colValue=data.getString(colName);
                                    colObj.put(colName, colValue);
                                }

                                HbaseUtil.putRow(
                                        hbaseConn,
                                        Constant.HBASE_NAMESPACE,
                                        tableProcessDim.getSinkTable(),
                                        tableProcessDim.getSinkFamily(),
                                        rowKay,
                                        colObj
                                );
                            }
                        }
                        collector.collect(Tuple2.of(jsonObject,tableProcessDim));

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {

                        BroadcastState<String, TableProcessDim> broadcastState=context.getBroadcastState(mapStateDescriptor);

                        String op=tableProcessDim.getOp();
                        String sourceTable=tableProcessDim.getSourceTable();

                        //更新Map（预加载数据）、状态数据
                        if ("d".equals(op)) {
                            dimTableProcessMap.remove(sourceTable);
                            broadcastState.remove(sourceTable);

                        } else {
                            dimTableProcessMap.put(sourceTable, tableProcessDim);
                            broadcastState.put(sourceTable, tableProcessDim);
                        }


                    }
                });

        connectDS.print();


        env.execute();

        //




    }
}
