package com.source.demo;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountAverageWithValueState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

    private ValueState<Tuple2<String, Long>> countAndSum;
    /** 注册状态，并初始化 */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Tuple2<String, Long>> descriptor = new ValueStateDescriptor<Tuple2<String, Long>>(
                "valueDescriptor",
                Types.TUPLE(Types.STRING, Types.LONG));
        countAndSum = getRuntimeContext().getState(descriptor);

    }
    @Override
    public void flatMap(
            Tuple2<String, Long> element,
            Collector<Tuple2<String, Long>> collector) throws Exception {
        //拿取当前key的状态值
        Tuple2<String, Long> currentState = countAndSum.value();
        //如果是空，则初始化
        if (currentState == null) {
            currentState = Tuple2.of(element.f0, 0L);
        }
        //不为空，则计算,f0为key出现的次数 , f1为key对应的value叠加值
        currentState.f1 += element.f1;
        countAndSum.update(currentState);
        collector.collect(Tuple2.of(currentState.f0, currentState.f1));
    }
}
