package com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdExaminationTestExamQuestion {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10011, 4);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        tabEnv.executeSql(SqlUtil.readOdsDbSql(Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION));
        Table testExamQuestion = tabEnv.sqlQuery("select \n" +
                "`data`['exam_id'] exam_id,\n" +
                "`data`['paper_id'] paper_id,\n" +
                "`data`['question_id'] question_id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['is_correct'] is_correct,\n" +
                "`ts`\n" +
                "from topic_db\n" +
                "where `type`='insert' and `table` = 'test_exam_question'");
        tabEnv.createTemporaryView("test_exam_question", testExamQuestion);
        Table testExam = tabEnv.sqlQuery("select \n" +
                "`data`['id'] exam_id,\n" +
                "`data`['score'] exam_score,\n" +
                "`data`['duration_sec'] exam_duration_sec\n" +
                "from topic_db\n" +
                "where `type`='insert' and `table` = 'test_exam'");
        tabEnv.createTemporaryView("test_exam", testExam);
        Table dwdExaminationTestExamQuestion = tabEnv.sqlQuery("select\n" +
                "user_id,\n" +
                "question_id,\n" +
                "q.exam_id,\n" +
                "exam_score,\n" +
                "exam_duration_sec,\n" +
                "paper_id,\n" +
                "is_correct,\n" +
                "ts\n" +
                "from test_exam_question q\n" +
                "join test_exam e on q.exam_id=e.exam_id");
        tabEnv.executeSql("create table " + Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION + "(\n" +
                "user_id string,\n" +
                "question_id string,\n" +
                "exam_id string,\n" +
                "exam_score string,\n" +
                "exam_duration_sec string,\n" +
                "paper_id string,\n" +
                "is_correct string,\n" +
                "ts bigint,\n" +
                "primary key(user_id) not enforced " +
                ")" + SqlUtil.upsertKafkaConnector(Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION, 4));
        dwdExaminationTestExamQuestion.executeInsert(Constant.DWD_EXAMINATION_TEST_EXAM_QUESTION);
    }
}
