package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	uuid "github.com/nu7hatch/gouuid"
	kafka "github.com/segmentio/kafka-go"
	couchdb "github.com/zemirco/couchdb"
)

var (
	kafkaServer   = "localhost:9092"
	kafkaTopic    = "golang-events"
	couchServer   = "http://127.0.0.1:5984/"
	couchBasedata = "basedata"
)

var couchClient *couchdb.Client

type couchDocument struct {
	couchdb.Document
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
	ID        string `json:"id"`
}

func couchWriter(kafkaMessage string, kafkaTime int64, kafkaID string) {
	useDatabase := couchClient.Use(couchBasedata)
	docToWrite := &couchDocument{
		Message:   kafkaMessage,
		Timestamp: kafkaTime,
		ID:        kafkaID,
	}

	writeResult, err := useDatabase.Post(docToWrite)

	if err != nil {
		log.Fatal("[COUCH ERROR]", err)
	}

	fmt.Println(writeResult)
}

func kafkaReader(kafkaURL, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	url, _ := url.Parse(couchServer)
	couchClient, _ = couchdb.NewClient(url)
	reader := kafkaReader(kafkaServer, kafkaTopic)

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		reader.Close()
	}()

	for {
		msg, err := reader.ReadMessage(context.Background())

		if err != nil {
			log.Fatal("[KAFKA ERROR]", err)
		}

		kafkaMessage := string(msg.Value)
		kafkaTime := msg.Time.Unix()
		kafkaID, _ := uuid.NewV4()

		fmt.Println("New message from Kafka:", kafkaMessage, "Time:", kafkaTime, "ID", kafkaID.String())
		couchWriter(kafkaMessage, kafkaTime, kafkaID.String())
	}
}
