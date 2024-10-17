package com.source.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 用 CliFrontend 提交任务前，需要通过 nc -lk 9999 启动 socket 端口
 *
 */


public class SocketWordCountStreamGraph {
    public static void main(String[] args) throws Exception{
        /**
         * 创建StreamExecutionEnvironment
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("./a_conf/a.txt","cache");

        /** 设置检查点的时间间隔 */
        //env.enableCheckpointing(30000);
        /** 设置检查点路径*/
        //env.getCheckpointConfig().setCheckpointStorage("file:///H:/chk");
        // 状态后端设置
        // 设置存储文件位置为 file:///Users/flink/checkpoints
       /* RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file:///H:/chkcheckpoints", true);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(rocksDBStateBackend);*/
        env.setParallelism(2);
        env.setMaxParallelism(2);

        /** 读取socket数据 */
        DataStreamSource<String> fileStream =   env.socketTextStream("127.0.0.1",9999);
        /** 将数据转成小写 */
        SingleOutputStreamOperator<String> mapStream = fileStream.map(String :: toUpperCase);
        mapStream.setDescription("aaaaaaa");
        /** 按照空格切分字符串*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> flatMapStream = mapStream.flatMap(new Split());
        flatMapStream.setDescription("bbbbb");

        /** 分组聚合*/
        KeyedStream<Tuple2<String,Integer>,String> keyStream = flatMapStream.keyBy(value -> value.f0);
        /** 聚合*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> sumStream = keyStream.sum(1);
        //sumStream.cache();
        /** 打印 */
        DataStreamSink<Tuple2<String,Integer>> sink = sumStream.print();
        /** 执行任务 */
        env.execute("WordCount");
    }

    public static class Split implements FlatMapFunction<String, Tuple2<String,Integer>> {

        /**
         * 按照空格切分数据
         * @param element
         * @param collector
         * @throws Exception
         */
        @Override
        public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String [] eles = element.split(" ");
            for(String chr : eles){
                //Thread.sleep(10000);//睡眠十秒
                collector.collect(new Tuple2<>(chr,1));
            }
        }
    }
}


