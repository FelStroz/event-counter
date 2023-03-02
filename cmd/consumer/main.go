package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

var (
	count        bool
	size         int
	outputDir    string
	amqpUrl      string
	amqpExchange string
	declareQueue bool
	consume      bool
	lifetime     int
)

func init() {
	flag.BoolVar(&count, "count", true, "Cria resumo das mensagens geradas")
	flag.BoolVar(&consume, "consume", true, "Consome as mensagens do rabbitmq")
	flag.StringVar(&outputDir, "count-out", ".", "Caminho de saida do resumo")
	flag.IntVar(&size, "size", 20, "Quantidade de mensagens a serem lidas por vez")
	flag.StringVar(&amqpUrl, "amqp-url", "amqp://guest:guest@localhost:5672", "URL do RabbitMQ")
	flag.StringVar(&amqpExchange, "amqp-exchange", "eventcountertest", "Exchange do RabbitMQ")
	flag.BoolVar(&declareQueue, "amqp-declare-queue", true, "Declare fila no RabbitMQ")
	flag.IntVar(&lifetime, "lifetime", 5, "Tempo de vida do processo antes de ser encerrado (0s=infinite)")
}

func main() {
	flag.Parse()
	if consume {
		wg := &sync.WaitGroup{}
		consumerInstance := NewConsumer()
		consumer := &eventcounter.ConsumerWrapper{
			Consumer: consumerInstance,
		}

		if err := consumerInstance.SetConsumerConnection(); err != nil {
			log.Fatalf("can`t set consumer connection, err: %s", err.Error())
		}

		if declareQueue {
			if err := consumerInstance.Declare(); err != nil {
				log.Fatalf("can`t declare queue or exchange, err: %s", err.Error())
			}
		}

		consumerInstance.Channels = map[eventcounter.EventType]chan eventcounter.MessageConsumed{
			eventcounter.EventCreated: make(chan eventcounter.MessageConsumed),
			eventcounter.EventDeleted: make(chan eventcounter.MessageConsumed),
			eventcounter.EventUpdated: make(chan eventcounter.MessageConsumed),
		}

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			if err := consumerInstance.Consume(ctx, wg); err != nil {
				log.Fatalf("can't consume any message, err: %s", err.Error())
			}
		}()

		go func() {
			for created := range consumerInstance.Channels[eventcounter.EventCreated] {
				ctx := context.WithValue(ctx, "msg", created)
				_ = consumer.Created(ctx, created.Id)
			}
		}()

		go func() {
			for deleted := range consumerInstance.Channels[eventcounter.EventDeleted] {
				ctx := context.WithValue(ctx, "msg", deleted)
				_ = consumer.Deleted(ctx, deleted.Id)
			}
		}()

		go func() {
			for updated := range consumerInstance.Channels[eventcounter.EventUpdated] {
				ctx := context.WithValue(ctx, "msg", updated)
				_ = consumer.Updated(ctx, updated.Id)
			}
		}()

		time.Sleep(10 * time.Second)
		wg.Wait()

		if count {
			if err := Write(outputDir, consumerInstance.Counters); err != nil {
				log.Printf("can't right this message, err: %s", err.Error())
			}
		}

		cancel()
	}
}
