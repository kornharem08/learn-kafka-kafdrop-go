package main

import (
	"fmt"

	"github.com/IBM/sarama"
)

func main() {
	// Configure the consumer
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Println("Error creating consumer:", err)
		return
	}
	defer consumer.Close()

	// Subscribe to the topic
	partitionConsumer, err := consumer.ConsumePartition("my-topic", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("Error subscribing to topic:", err)
		return
	}
	defer partitionConsumer.Close()

	// Consume messages
	fmt.Println("Listening for messages...")
	for msg := range partitionConsumer.Messages() {
		fmt.Printf("Received message: %s (partition: %d, offset: %d)\n",
			string(msg.Value), msg.Partition, msg.Offset)
	}
}
