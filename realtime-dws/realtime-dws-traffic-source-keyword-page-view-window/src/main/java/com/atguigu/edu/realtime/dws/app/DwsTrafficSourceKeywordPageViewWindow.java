package com.atguigu.edu.realtime.dws.app;

import com.atguigu.edu.realtime.dws.function.KeywordUDTF;
import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/13  14:44
 */


public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) {
        //TODO 设置流处理环境 指定端口以及并行度
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(18024, 4);
        //指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 注册分词函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                "    common map<string,string>,\n" +
                "    page  map<string,string>,\n" +
                "    ts bigint,\n" +
                "    et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "    WATERMARK FOR et AS et\n" +
                ")" + SqlUtil.kafkaConnector(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_source_keyword_page_view_window"));
        // tableEnv.executeSql("select * from page_log").print();

        //TODO 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "    page['item'] fullword,\n" +
                "    et\n" +
                "from page_log \n" +
                "where   page['item_type']='keyword' and page['item'] is not null");
//        searchTable.execute().print();
        tableEnv.createTemporaryView("search_table", searchTable);
        //TODO 分词  并将分词结果和原表字段进行关联
//        select et,word from search_table t1 left join lateral table(ik_analyze(t1.fullword)) t2(word)
        Table splitable = tableEnv.sqlQuery("select et,keyword from search_table t1 , lateral table(ik_analyze(t1.fullword)) t2(keyword)");
        //splitable.execute().print();
        tableEnv.createTemporaryView("split_table",splitable);
        //TODO 分组 开窗  聚合计算
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "    keyword,\n" +
                "    count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY keyword,window_start, window_end");

        //resTable.execute().print();
        //TODO 将聚合的结果写到Doris表中
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                ")" + SqlUtil.dorisConnector("dws_traffic_source_keyword_page_view_window"));
        //6.2 写入
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");


    }
}
