package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionReviewInfo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10010, 4);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.executeSql(SqlUtil.readOdsDbSql("dwd_interaction_review_info"));
        Table reviewInfo = tabEnv.sqlQuery("select\n" +
                "\t`data`[user_id],\n" +
                "\t`data`[course_id],\n" +
                "\t`data`[review_stars],\n" +
                "\t`ts`\n" +
                "from topic_db\n" +
                "where `type`='insert'");
        tabEnv.executeSql("create table dwd_interaction_review_info(\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "review_stars bigint,\n" +
                "ts bigint\n" +
                ")" + SqlUtil.upsertKafkaConnector("dwd_interaction_review_info", 4));
        reviewInfo.executeInsert("dwd_interaction_review_info");
    }
}
