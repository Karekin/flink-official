package com.source.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 小Q
 * @Desctription: TODO
 * @Date: Created in 2024/4/14 1:51
 * @Version: 1.0
 */


public class SocketWordCountState {
    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Volumes/karekinSSD1/resource/FlinkSourceCode/FlinkCore");

        /**
         * 创建StreamExecutionEnvironment
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.registerCachedFile("./a_conf/a.txt","cache");
        // 启用非对齐 Checkpoint
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        /** 设置检查点的时间间隔 */
        //需要开启 Checkpoint 机制10000000
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(2);
        env.setMaxParallelism(2);

        /** 读取socket数据 */
        DataStreamSource<String> fileStream =   env.socketTextStream("127.0.0.1",9999);
        // 3. 使用 map 函数将字符串拆分为单词，并输出 (word, 1) 的元组
        fileStream
                .map(new Tokenizer())
                .setParallelism(2)
                .keyBy(0) // 按单词进行分组
                .flatMap(new CountAverageWithValueState()).map(vale -> vale.f1).print(); // 使用自定义的 KeyedProcessFunction 来处理状态


        // 4. 输出结果（这里只是打印到控制台，你可以替换为输出到 Kafka、文件等）

        // 5. 执行 Flink 作业
        env.execute("Flink Word Count with State");
    }

    // Tokenizer 是一个简单的 map 函数，用于将字符串拆分为单词
    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(String value) {
            return new Tuple2<>(value, 1l);
        }
    }


}


