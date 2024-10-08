package com.atguigu.educate.realtime.common.constant;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String DORIS_FE_NODES = "8.130.101.83:9030";//hadoop107:7030  localhost:10000    8.130.101.83:7030
    public static final String DORIS_DATABASE = "educate-realtime";
    public static final String DORIS_PASSWORD = "educate-realtime0318";


    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String HBASE_NAMESPACE = "educate";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";


}

