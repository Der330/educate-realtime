package com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionReviewInfo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10010, 4);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.executeSql(SqlUtil.readOdsDbSql(Constant.DWD_INTERACTION_REVIEW_INFO));
        Table reviewInfo = tabEnv.sqlQuery("select\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['course_id'] course_id,\n" +
                "\t`data`['review_stars'] review_stars,\n" +
                "\t`ts`\n" +
                "from topic_db\n" +
                "where `type`='insert' and `table` = 'review_info'");
        tabEnv.executeSql("create table " + Constant.DWD_INTERACTION_REVIEW_INFO + "(\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "review_stars string,\n" +
                "ts bigint,\n" +
                "primary key(user_id) not enforced " +
                ")" + SqlUtil.upsertKafkaConnector(Constant.DWD_INTERACTION_REVIEW_INFO, 4));
        reviewInfo.executeInsert(Constant.DWD_INTERACTION_REVIEW_INFO);
    }
}
