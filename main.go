package main

import (
	"context"
	"encoding/json"
	"fmt"
	"iscreen/go-kafka-microservice/dto"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

func main() {
	sendMessage()
	receiveMessage()
}

func sendMessage() {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "order_created",
		Balancer: &kafka.LeastBytes{},
	}

	order := dto.CreateOrderDto{
		OrderId: "1222",
		UserId:  "123",
		Price:   decimal.NewFromFloat32(22.5),
	}
	bytesOrder, err := json.Marshal(order)
	if err != nil {
		log.Fatal("failed to convert messages:", err)
	}
	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: bytesOrder,
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func receiveMessage() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "billing-consumer",
		Topic:    "order_created",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	fmt.Println("Start waiting kafka...")
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		createOrder := dto.CreateOrderDto{}
		json.Unmarshal(m.Value, &createOrder)
		fmt.Printf("orderId: %s, userId: %s, price: %s \n", createOrder.OrderId, createOrder.UserId, createOrder.Price)
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
