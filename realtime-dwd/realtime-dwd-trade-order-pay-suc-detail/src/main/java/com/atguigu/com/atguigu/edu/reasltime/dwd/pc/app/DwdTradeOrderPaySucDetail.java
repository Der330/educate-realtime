package com.atguigu.com.atguigu.edu.reasltime.dwd.pc.app;

import com.atguigu.educate.realtime.common.base.BaseSQLApp;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        // TODO 1. 读取下单事务事实表
        tableEnv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                "   id string,\n" +
                "  order_id string,\n" +
                "  user_id string,\n" +
                "  course_id string,\n" +
                "  course_name string,\n" +
                "   origin_amount string,   \n" +
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
                "  et as to_timestamp_ltz(ts,0),\n" +
                " watermark for et as et -interval '3' second \n" +
                ")" + SqlUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        // tableEnv.executeSql("select * from dwd_trade_order_detail").print();

        // TODO 2. 读取 topic_db
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        // TODO 3. 从 topic_db 中过滤 payment_info
        Table paymentInfo = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id ,\n" +
                "  `data`['out_trade_no'] out_trade_no,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['alipay_trade_no'] alipay_trade_no,\n" +
                "  `data`['total_amount'] total_amount,\n" +
                "  `data`['trade_body'] trade_body ,\n" +
                "  `data`['payment_type'] payment_type ,\n" +
                "  `data`['payment_status'] payment_status ,\n" +
                "  `data`['create_time'] create_time,\n" +
                "  `data`['callback_time'] callback_time,\n" +
                "  ts ,\n" +
                "  pt ,\n" +
                "  et \n" +
                " from topic_db where `type`='insert' \n" +
                "and `table`='payment_info' ");

        // paymentInfo.execute().print();
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // TODO 4. 2张join: interval join 无需设置 ttl
        Table result = tableEnv.sqlQuery("select \n" +
                " dto.order_id,\n" +
                " dto.user_id,\n" +
                " dto.course_id,\n" +
                " dto.course_name,\n" +
                " dto.origin_amount,\n" +
                " dto.coupon_reduce,\n" +
                " dto.final_amount,\n" +
                " dto.origin_amount_all,\n" +
                " dto.coupon_reduce_all,\n" +
                " dto.final_amount_all,\n" +
                " dto.order_status,\n" +
                " dto.date_id,\n" +
                " dto.ts,\n" +
                " dto.session_id,\n" +
                " dto.province_id,\n" +
                " pi.out_trade_no,\n" +
                " pi.alipay_trade_no,\n" +
                " pi.total_amount,\n" +
                " pi.trade_body,\n" +
                " pi.payment_type,\n" +
                " pi.payment_status,\n" +
                " pi.create_time,\n" +
                " pi.callback_time \n" +
                "from payment_info pi  join   dwd_trade_order_detail dto \n" +
                "on pi.order_id = dto.order_id \n" +
                "and dto.et >= pi.et -interval '30' minute \n" +
                "and dto.et <=pi.et + interval '5' second");


        //todo 5 写到kafka
        tableEnv.executeSql("create table dwd_trade_order_payment_success(\n" +
                "  order_id string,\n" +
                "  user_id string,\n" +
                "  course_id string,\n" +
                "  course_name string,\n" +
                "  origin_amount string,\n" +
                "  coupon_reduce string,\n" +
                "  final_amount string,\n" +
                "  origin_amount_all string,\n" +
                "  coupon_reduce_all string,\n" +
                "  final_amount_all string,\n" +
                "  order_status string,\n" +
                "  date_id string,\n" +
                "  ts bigint,\n" +
                "  session_id string,\n" +
                "  province_id string,\n" +
                "  out_trade_no string,\n" +
                "  alipay_trade_no string,\n" +
                "  total_amount string,\n" +
                "  trade_body string,\n" +
                "  payment_type string,\n" +
                "  payment_status string,\n" +
                "  create_time string,\n" +
                "  callback_time string,\n" +
                "  primary key(order_id) not enforced\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}