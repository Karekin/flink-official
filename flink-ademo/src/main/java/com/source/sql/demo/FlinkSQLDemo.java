package com.source.sql.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class FlinkSQLDemo {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 创建 TableEnvironment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建数据流
        final DataStream<Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "rubber", 2)));

        // 将数据流转为 Table，并注册成临时表
        final Table tableA = tableEnv.fromDataStream(orderA);
        tableEnv.createTemporaryView("Orders", tableA);

        // 执行 SQL 查询
        final Table result = tableEnv.sqlQuery("SELECT user, product FROM Orders WHERE user = 1");

        // 将结果转换为数据流并打印
        /*
            在 toDataStream(result, Order.class) 中，你将查询结果转换回 DataStream，
            但由于你的 SQL 查询只选择了 user 和 product 两列，而没有包含 amount 列，
            所以无法将其转换为 Order 类型，故改为 SimpleOrder 类型
         */
        tableEnv.toDataStream(result, SimpleOrder.class).print();

        // 执行任务
        env.execute("Flink SQL Demo");
    }

    // Order 类
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        // for POJO detection in DataStream API
        public Order() {}

        // for structured type detection in Table API
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user=" + user
                    + ", product='" + product + '\''
                    + ", amount=" + amount
                    + '}';
        }
    }

    // SimpleOrder 类，用于存储 SQL 查询结果
    public static class SimpleOrder {
        public Long user;
        public String product;

        public SimpleOrder() {}

        public SimpleOrder(Long user, String product) {
            this.user = user;
            this.product = product;
        }

        @Override
        public String toString() {
            return "SimpleOrder{"
                    + "user=" + user
                    + ", product='" + product + '\''
                    + '}';
        }
    }
}
