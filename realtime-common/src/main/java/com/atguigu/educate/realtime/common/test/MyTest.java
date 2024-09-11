package com.atguigu.educate.realtime.common.test;

import com.atguigu.educate.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class MyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }

                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes != null) {
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }
                        }
                )
                .setGroupId("topic")
                .setTopics(Constant.TOPIC_DB)
                .build(), WatermarkStrategy.noWatermarks(), "test").print();
        env.execute();
    }
}
