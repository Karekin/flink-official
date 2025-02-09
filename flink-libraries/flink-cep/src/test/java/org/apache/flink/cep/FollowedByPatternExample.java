package org.apache.flink.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FollowedByPatternExample {

    public static class Event {
        public String name;
        public int id;

        public Event(String name, int id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public String toString() {
            return "Event{name='" + name + "', id=" + id + "}";
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构造数据源
        DataStream<Event> input = env.fromData(
                new Event("start", 1),
                new Event("noise-1", 0),
                new Event("noise-2", 0),
                new Event("middle", 2),
                new Event("end", 3)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()));

        // 定义模式：宽松连续 "start" -> "middle"
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event ->
                        event.name.equals("start")
                ))
                .followedBy("middle")
                .where(SimpleCondition.of(event ->
                        event.name.equals("middle")
                ))
                .optional();


        // 将模式应用到数据流上
        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                Event start = match.get("start").get(0);
                List<Event> middleEvents = match.get("middle"); // 获取 middle 事件
                if (middleEvents != null && !middleEvents.isEmpty()) {
                    out.collect("Matched: " + start + " -> " + middleEvents.get(0));
                } else {
                    out.collect("Matched: " + start + " -> (middle skipped)");
                }
            }
        }).print();


        env.execute("Flink FollowedByPattern Example");
    }


}

