package com.atguigu.educate.realtime.common.util;


import com.atguigu.educate.realtime.common.constant.Constant;

public class SqlUtil {
    public static String kafkaConnector(String topic, String groupId) {
        String sql =
                "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"',\n" +
                "  'properties.group.id' = '"+groupId+"',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                //"  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        return sql;
    }

    public static String upsertKafkaConnector(String topic, int parallelism) {
        String sql =
                "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json',\n" +
                "  'sink.parallelism' = '"+parallelism+"'\n" +
                ")";
        return sql;
    }

    //获取HBase连接器相关连接属性
    public static String hBaseConnector(String tableName){
        return " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+Constant.HBASE_NAMESPACE+":"+tableName+"',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    public static String dorisConnector(String tableName){

        String sql = "WITH (\n" +
                "  'connector' = 'doris', " +
                "  'fenodes' = '"+Constant.DORIS_FE_NODES+"', " +
                "  'table.identifier' = '"+Constant.DORIS_DATABASE+"."+tableName+"', " +
                "  'username' = 'root', " +
                "  'password' = '"+Constant.DORIS_PASSWORD+"', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ")";

        return sql;
    }

    public static String readOdsDbSql(String groupId){
        String sql =
                "CREATE TABLE topic_db (\n" +
                        "  `database` string,\n" +
                        "  `table` string,\n" +
                        "  `type` string,\n" +
                        "  `data` map<string,string>,\n" +
                        "  `old` map<string,string>,\n" +
                        "  ts bigint,\n" +
                        "  pt as proctime(),\n" +
                        "  et as TO_TIMESTAMP_LTZ(ts, 0),\n" +
                        "  WATERMARK FOR et AS et \n" +
                        ") " + kafkaConnector(Constant.TOPIC_DB,groupId);
        return sql;
    }

    public  static String readDicSql(String tableName) {
        String sql = "CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + hBaseConnector(tableName);

        return sql;
        //tableEnv.executeSql("select dic_code,dic_name from base_dic").print();
    }

    public static String getKafkaDDL(String topic , String groupId){
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topic +"',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId +"',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                //     "  'properties.auto.offset.reset' = 'latest'\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getHBaseDDL(String tableName ){
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+ Constant.HBASE_NAMESPACE +":" + tableName +"',\n" +
                " 'zookeeper.quorum' = 'hadoop103,hadoop104,hadoop105:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic){
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    public static String getDorisDDL(String tableName){
        return "WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = '" + Constant.DORIS_FE_NODES + "', " +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + "."  + tableName +"', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ") " ;
    }



}
