package com.lumi.lens;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class KafkaOrderStats {

    // 创建 SLF4J 日志记录器
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderStats.class);

    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Kafka 消费者
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.18.201:9092"); // 替换为 Kafka 地址
        kafkaProps.setProperty("group.id", "order-stats-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "order-topic",                // Kafka 主题名称
                new SimpleStringSchema(),     // 处理 Kafka 消息的 Schema
                kafkaProps
        );

        // 3. 添加 Kafka 消费者
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // 在消费 Kafka 消息时增加日志
        kafkaStream.map(json -> {
            // LOG.info("Received Kafka message: {}", json); // 打印收到的 Kafka 消息
            return json;
        });

        // 4. 转换数据流并分配时间戳和水位线
        DataStream<Order> ordersStream = kafkaStream
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    Order order = mapper.readValue(json, Order.class); // JSON 转换为 Order 对象
                    LOG.info("Parsed Order 1059: {}", order); // 打印解析后的订单对象
                    return order;
                })
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            LOG.info("Assigning timestamp: {}, Current Watermark: {}", event.timestamp, timestamp);
                            return event.timestamp;
                        })
                        .withIdleness(Duration.ofSeconds(10)) // Optional: 如果数据流间隔较长，防止水印卡住

        );

        DataStream<Tuple2<String, Double>> resultStream = ordersStream
                .keyBy(order -> order.category)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<Order, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String category, Context context, Iterable<Order> elements, Collector<Tuple2<String, Double>> out) {
                        double total = 0;
                        LOG.info("Window triggered for category: {} at {}", category, context.window().getEnd());

                        for (Order order : elements) {
                            LOG.info("Processing Order - Category: {}, Price: {}, Timestamp: {}", order.category, order.price, order.timestamp);
                            total += order.price;
                        }
                        Tuple2<String, Double> result = new Tuple2<>(category, total);
                        LOG.info("Window result - Category: {}, Total: {}", category, total);
                        out.collect(result);
                    }
                });

        // 6. 配置 Kafka 生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "order-stats-topic",  // 输出 Kafka 主题
                new SimpleStringSchema(),
                kafkaProps
        );

        // 7. 将结果发送到 Kafka
        resultStream
                .map(result -> {
                    String output = "Category: " + result.f0 + ", Total: " + result.f1;
                    LOG.info("Sending result to Kafka: {}", output); // 打印发送到 Kafka 的数据
                    return output;
                })
                .addSink(kafkaProducer); // 发送到 Kafka

        // 8. 启动作业
        env.execute("Kafka Order Stats");
    }

    // 定义订单类
    public static class Order {
        public String orderId;
        public String productId;
        public String productName;
        public String category;
        public double price;
        public long timestamp;

        // 必须提供无参构造函数供 Jackson 反序列化使用
        public Order() {
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId='" + orderId + '\'' +
                    ", productId='" + productId + '\'' +
                    ", productName='" + productName + '\'' +
                    ", category='" + category + '\'' +
                    ", price=" + price +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}

