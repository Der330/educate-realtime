package com.atguigu.educate.realtime.common.constant;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop104:9092,hadoop105:9092,hadoop106:9092";//hadoop104:9092

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String TOPIC_START_TAG = "startTag";
    public static final String TOPIC_PAGE_TAG = "pageTag";
    public static final String TOPIC_APP_VIDEO = "appVideo";

    public static final String TOPIC_START_TAG = "topic_start_tag";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "topic_dwd_traffic_page";

    public static final String DORIS_FE_NODES = "8.130.101.83:7030";//hadoop107:7030  localhost:10000    8.130.101.83:7030
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

    public static final String DWD_USER_LOGIN_INFO = "dwd_user_login_info";

    public static final String DWD_EXAMINATION_TEST_EXAM_QUESTION = "dwd_examination_test_exam_question";

    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String DWS_COURSE_COURSE_ORDER_WINDOW = "dws_course_course_order_window";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";

    public static final String DWS_EXAMINATION_PAPER_TEST_WINDOW = "dws_examination_paper_test_window";
    public static final String DWS_INTERACTION_COURSE_REVIEW_WINDOW = "dws_interaction_course_review_window";
    public static final String DWS_EXAMINATION_COURSE_TEST_WINDOW = "dws_examination_course_test_window";
    public static final String DWS_EXAMINATION_QUESTION_TEST_WINDOW = "dws_examination_question_test_window";

    public static final String DWS_EXAMINATION_PAPER_SCORE_LEVEL_TEST_WINDOW = "dws_examination_paper_score_level_test_window";

    public static final String TOPIC_PAGE_TAG = "pageTag";
    public static final String TOPIC_APP_VIDEO = "appVideo";
}

