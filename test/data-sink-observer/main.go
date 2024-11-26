package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

func main() {
	// Kafka 配置
	broker := "localhost:9092" // 替换为你的 Kafka 地址
	topic := "order-stats-topic"

	// 创建生产者用于检查和创建主题
	producer, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Fatalf("Failed to create admin client: %s", err)
	}
	defer producer.Close()

	// 检查主题是否存在并尝试创建
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := producer.CreateTopics(ctx,
		[]kafka.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	)
	if err != nil {
		log.Printf("Failed to create topic '%s': %s", topic, err)
	} else {
		for _, result := range results {
			if result.Error.Code() == kafka.ErrNoError {
				log.Printf("Topic '%s' created successfully.", result.Topic)
			} else {
				log.Printf("Failed to create topic '%s': %s", result.Topic, result.Error.String())
			}
		}
	}

	// 创建消费者
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "order-stats-consumer-group",
		"auto.offset.reset": "earliest", // 从最早的消息开始消费
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// 订阅主题
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	log.Printf("Listening to topic: %s", topic)

	// 消费消息
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v (%v)", err, msg)
			continue
		}
		// 打印消息内容到标准输出
		fmt.Printf("Received message: %s\n", string(msg.Value))
	}
}

