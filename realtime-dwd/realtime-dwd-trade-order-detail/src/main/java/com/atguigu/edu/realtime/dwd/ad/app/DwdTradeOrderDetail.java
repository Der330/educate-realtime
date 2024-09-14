package com.atguigu.edu.realtime.dwd.ad.app;

import com.atguigu.educate.realtime.common.base.BaseSQLApp;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 1.从kafka的topic_db主题中读取数据
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        // 设置状态的保留时间    传输的延迟 + 业务上的滞后关系
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 2.过滤出订单明细数据
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id ,\n" +
                "  `data`['course_id'] course_id ,\n" +
                "  `data`['course_name'] course_name  ,\n" +
                "  `data`['order_id'] order_id ,\n" +
                "  `data`['user_id'] user_id ,\n" +
                "  `data`['origin_amount'] origin_amount ,\n" +
                "  `data`['coupon_reduce'] coupon_reduce ,\n" +
                "  `data`['final_amount'] final_amount ,\n" +
                "  ts\n" +
                " from topic_db\n" +
                " where `type` ='insert' and `table` ='order_detail' and `database` ='edu' ");
        // orderDetail.execute().print();
        tableEnv.createTemporaryView("order_detail", orderDetail);
        // TODO 3.过滤出订单数据
        Table orderInfo = tableEnv.sqlQuery(" select \n" +
                "  `data`['id'] id ,\n" +
                "  `data`['user_id'] user_id ,\n" +
                "  `data`['origin_amount'] origin_amount ,\n" +
                "  `data`['final_amount'] final_amount ,\n" +
                "  `data`['coupon_reduce'] coupon_reduce ,\n" +
                "  `data`['order_status'] order_status ,\n" +
                "  `data`['out_trade_no'] out_trade_no ,\n" +
                "  `data`['trade_body'] trade_body ,\n" +
                "  `data`['session_id'] session_id ,\n" +
                "  `data`['province_id'] province_id ,\n" +
                "  `data`['create_time'] create_time ,\n" +
                "  `data`['expire_time'] expire_time ,\n" +
                "  `data`['update_time'] update_time, \n" +
                "  ts \n" +
                " from topic_db \n" +
                " where (`type` ='insert' or (`type` ='update' and `old`['order_status'] is not null ))\n" +
                " and `table` ='order_info' and `database` ='edu' ");
        // orderInfo.execute().print();
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 4.关联上述2张表
        Table result = tableEnv.sqlQuery("select \n" +
                " od.order_id,\n" +
                " od.id,\n" +
                " od.course_id,\n" +
                " od.course_name,\n" +
                " od.user_id,\n" +
                " od.origin_amount,\n" +
                " od.coupon_reduce,\n" +
                " od.final_amount,\n" +
                " oi.origin_amount origin_amount_all,\n" +
                " oi.coupon_reduce coupon_reduce_all,\n" +
                " oi.final_amount final_amount_all,\n" +
                " oi.order_status,\n" +
                " oi.out_trade_no,\n" +
                " date_format(oi.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                " oi.session_id,\n" +
                " oi.province_id,\n" +
                " oi.ts\n" +
                " from \n" +
                " order_detail od \n" +
                " join order_info oi  \n" +
                " on \n" +
                " oi.id =od.order_id");
        // result.execute().print();
        // TODO 5.将关联的结果写到kafka对应的主题
        tableEnv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                "  order_id string,\n" +
                "   id string,\n" +
                "  course_id string,\n" +
                "  course_name string,\n" +
                "  user_id string,\n" +
                "  origin_amount string,   \n" +
                "  coupon_reduce string,\n" +
                "  final_amount string,\n" +
                "  origin_amount_all string,\n" +
                "  coupon_reduce_all string,\n" +
                "  final_amount_all string,\n" +
                "  order_status string,\n" +
                "  out_trade_no string,\n" +
                "  date_id string,\n" +
                "  session_id string,\n" +
                "  province_id string,\n" +
                "  ts bigint,\n" +
                "  primary key(id) not enforced \n" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}