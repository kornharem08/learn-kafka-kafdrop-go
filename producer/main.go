package main

import (
	"fmt"

	"github.com/IBM/sarama" // Updated import path
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Println("Error creating producer:", err)
		return
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: "my-topic",
		Value: sarama.StringEncoder("Hello, Kafka from Go!!!!!"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
