package com.atguigu.educate.realtime.common.util;

import com.atguigu.educate.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
<<<<<<< HEAD
=======
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
>>>>>>> origin/dev
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

<<<<<<< HEAD
/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/13  10:24
 */


public class FlinkSourceUtil {
    //获取KafkaSource
=======
public class FlinkSourceUtil {
>>>>>>> origin/dev
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
<<<<<<< HEAD
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用SimpleStringSchema读取kafka主题数据的时候，如果读取到的数据是空消息，处理不了，需要重写反序列化器
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if(message != null){
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
=======
                //从flink维护的偏移量位置开始消费
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用的是SimpleStringSchema对kafka消息做反序列化，如果读取到的消息为空，会报错
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                //如果要处理空消息的话，需要自定义反序列化器
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
>>>>>>> origin/dev
                .build();
        return kafkaSource;
    }

    //获取MySqlSource
<<<<<<< HEAD
    public static MySqlSource<String> getMySqlSource(String database, String tableName){
=======
    public static MySqlSource<String> getMySqlSource(String database, String table){
>>>>>>> origin/dev
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
<<<<<<< HEAD
                //在程序启动的时候，先对指定的库的表进行全表扫描(同步历史数据，和binlog没有关系)，然后在从binlog的最新位置读取变化数据
                //.startupOptions(StartupOptions.initial())
                .databaseList(database)
                .tableList(database + "." + tableName)
=======
                .databaseList(database)
                .tableList(database + "." + table)
                .startupOptions(StartupOptions.initial())
>>>>>>> origin/dev
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
