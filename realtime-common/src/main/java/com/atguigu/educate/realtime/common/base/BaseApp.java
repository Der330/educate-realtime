package com.atguigu.educate.realtime.common.base;

import com.atguigu.educate.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseApp {
    public void start(int port, int parallelism, String groupId, String topic){
        //TODO 1.环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(parallelism);

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置检查点超时时间
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointTimeout(60000L);
//        //2.3 设置job取消后检查点是否保留
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查点之间时间间隔
//        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
//        //2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.6 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // //2.7 设置检查点存储路径
        // checkpointConfig.setCheckpointStorage("hdfs://hadoop101:8020/ck/" + groupId);
        // //2.8 设置操作Hadoop的用户
        // System.setProperty("HADOOP_USER_NAME","atguigu");


        //TODO 3.从kafka的主题中读取数据
        //3.1 声明消费的主题以及消费者组

        //3.2 创建KafkaSource消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_Source");

        //TODO 4.业务处理
        handle(env,kafkaStrDS);

        //TODO 5.提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public  abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) ;
}

