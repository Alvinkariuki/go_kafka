package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Represents structure of message in a transaction
type TransactionalMessage struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

func main() {
	// Generate unique transactional ID for this producer instance
	transactionalID := generateTransactionalID()

	// Kafka producer configuration with exactly one semantic enabled
	config := &kafka.ConfigMap{
		"bootstrap.servers":                     "localhost:9092,localhost:9093,localhost:9094",
		"acks":                                  "all",
		"enable.idempotence":                    true,
		"transactional.id":                      transactionalID,
		"max.in.flight.requests.per.connection": 1,
	}

	// Initialize Kafka producer
	producer, err := kafka.NewProducer(config)

	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Initialize Kafka transaction
	err = producer.InitTransactions(context.TODO())
	if err != nil {
		log.Fatalf("Failed to initialize transactions: %s", err)
	}

	// Begin Kafka transaction
	err = producer.BeginTransaction()

	if err != nil {
		log.Fatalf("Failed to begin transaction: %s", err)
	}

	// Create sample message
	message := TransactionalMessage{ID: "msg_001", Content: "Hello Kafka with Exactly Once Semantics"}

	// Serialize message to JSON
	messageBytes, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("Failed to serialize message %s", err)
	}

	// Producer message to Kafka topic
	topic := "exactly-once-topic"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageBytes,
	}, nil)

	if err != nil {
		log.Fatalf("Failed to produce message: %s", err)
	}

	// Commit the transaction to ensure the message is sent exactly once
	err = producer.CommitTransaction(context.TODO())

	if err != nil {
		log.Fatalf("Failed to commit transaction: %s", err)
	}

	log.Println("Message sent successfully with Exactly Once Semantics")
}

func generateTransactionalID() string {
	hostname, _ := os.Hostname()
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s-%d", hostname, timestamp)
}
