package main

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TransactionalMessage struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

func main() {
	// Create a new consumer configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092,localhost:9093,localhost:9094",
		"group.id":           "exactly-once-consumer-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	}

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to topic
	topic := "exactly-once-topic"
	err = consumer.Subscribe(topic, nil)

	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	log.Println("Consumer started, waiting for messages...")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v", err)
			continue
		}

		// Deserialize message
		var transactionMessage TransactionalMessage
		err = json.Unmarshal(msg.Value, &transactionMessage)
		if err != nil {
			log.Printf("Failed to deserialize message: %v", err)
			continue
		}

		// Simulate processing the message
		processMessage(transactionMessage)

		// Commit the message's offset after successful processing
		kafkaCommit, err := consumer.CommitMessage(msg)
		if err != nil {
			log.Printf("Failed to commit message: %v", err)
		}

		log.Printf("Commit success: Offset=%s, Partition=%s", kafkaCommit[0].Offset, kafkaCommit[0].Partition)
	}

}

func processMessage(message TransactionalMessage) {
	log.Printf("Processing message: ID=%s, Content=%s", message.ID, message.Content)
}
