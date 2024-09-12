package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.educate.realtime.common.constant.Constant;
import com.atguigu.educate.realtime.common.util.FlinkEnvUtil;
import com.atguigu.educate.realtime.common.util.FlinkKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsInteractionCourseReviewWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkEnvUtil.getEnv(10100, 4);
        env.fromSource(FlinkKafkaUtil.getKafkaSource(Constant.DWD_INTERACTION_REVIEW_INFO,))
    }
}
