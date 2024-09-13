package com.atguigu.com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.educate.realtime.common.base.BaseSQLApp;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // todo 1 从topic_db主题读取数据
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
        // tableEnv.executeSql("select `data` from topic_db").print();

        // TODO 2.过滤出加购行为
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['course_id'] course_id ,\n" +
                " `data`['cart_price'] cart_price,\n" +
                " `data`['session_id'] session_id,\n" +
                " `data`['deleted'] deleted,\n" +
                " `data`['sold'] sold ,\n" +
                " ts \n" +
                " from topic_db\n" +
                " where `type` ='insert' and `table` ='cart_info' and `database` ='edu' and `data`['sold']='1'");
        // cartInfo.execute().print();
        tableEnv.createTemporaryView("cart_info",cartInfo);
        // TODO 3.将加购数据写到kafka主题
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_TRADE_CART_ADD+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    course_id string,\n" +
                "    cart_price string,\n" +
                "    session_id string,\n" +
                "    deleted string,\n" +
                "    sold string,\n" +
                "    ts bigint, \n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        tableEnv.executeSql("select * from dwd_trade_cart_add").print();

        //3.2 写入2
        tableEnv.executeSql("insert into "+Constant.TOPIC_DWD_TRADE_CART_ADD+" select * from cart_info");
    }
}
