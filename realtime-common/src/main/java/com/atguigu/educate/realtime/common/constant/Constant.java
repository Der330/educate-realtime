package com.atguigu.educate.realtime.common.constant;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop104:9092,hadoop105:9092,hadoop106:9092";//hadoop104:9092

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String DORIS_FE_NODES = "8.130.101.83:9030";//hadoop107:7030  localhost:10000    8.130.101.83:7030
    public static final String DORIS_DATABASE = "educate_realtime";
    public static final String DORIS_PASSWORD = "educate-realtime0318";

    public static final String HABSE_ZOOKEEPER_QUORUM = "hadoop104,hadoop105,hadoop106";
    public static final String HABSE_ROOTDIR = "hdfs://hadoop102:8020/hbase";
    public static final String MYSQL_HOST = "hadoop102";//hadoop102
    public static final int MYSQL_PORT = 3306;//3306
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String HBASE_NAMESPACE = "educate";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";

    public static final String DIM_CONFIG_TABLE = "table_process_dim";

    public static final String DWD_INTERACTION_REVIEW_INFO = "dwd_interaction_review_info";

    public static final String DWD_EXAMINATION_TEST_EXAM_QUESTION = "dwd_examination_test_exam_question";

    public static final String DWS_INTERACTION_COURSE_REVIEW_WINDOW = "dws_interaction_course_review_window";

}

