create table sinkTable(
id STRING,
user_id STRING,
origin_amount STRING,
final_amount STRING,
coupon_reduce STRING,
order_status STRING,
out_trade_no STRING,
trade_body STRING,
session_id STRING,
province_id STRING,
create_time STRING,
expire_time STRING,
update_time,STRING
ts BIGINT
)



    " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")    "

    "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"',\n" +
                "  'properties.group.id' = '"+groupId+"',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")"