package com.atguigu.atguigu.edu.realtime.dwd.app;

import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderInfo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10027, 4);

        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);

        TableResult topicDbSource=tableEnv.executeSql(SqlUtil.readOdsDbSql("DwdTradeOrderInfo"));


        Table orderInfo = tableEnv.sqlQuery(" select \n" +
                "  `data`['id'] id ,\n" +
                "  `data`['user_id'] user_id ,\n" +
                "  ts \n" +
                " from topic_db \n" +
                " where (`type` ='insert' or (`type` ='update' and `old`['order_status'] is not null ))\n" +
                " and `table` ='order_info' and `database` ='edu' ");


        tableEnv.createTemporaryView("orderInfo",orderInfo);
//        tableEnv.executeSql("select * from orderInfo").print();


        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_INFO + "(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "ts bigint,\n" +
                "primary key(user_id) not enforced " +
                ")" + SqlUtil.upsertKafkaConnector(Constant.TOPIC_DWD_TRADE_ORDER_INFO, 4));

        orderInfo.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_INFO);





    }
}
