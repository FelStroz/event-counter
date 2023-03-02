package main

import (
	"context"
	"flag"
	"fmt"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
	"log"
	"sync"
	"time"
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
	flag.BoolVar(&count, "count", false, "Cria resumo das mensagens geradas")
	flag.BoolVar(&consume, "consume", true, "Consome as mensagens do rabbitmq")
	flag.StringVar(&outputDir, "count-out", ".", "Caminho de saida do resumo")
	flag.IntVar(&size, "size", 20, "Quantidade de mensagens a serem lidas por vez")
	flag.StringVar(&amqpUrl, "amqp-url", "amqp://guest:guest@localhost:5672", "URL do RabbitMQ")
	flag.StringVar(&amqpExchange, "amqp-exchange", "eventcountertest", "Exchange do RabbitMQ")
	flag.BoolVar(&declareQueue, "amqp-declare-queue", true, "Declare fila no RabbitMQ")
	flag.IntVar(&lifetime, "lifetime", 5, "Tempo de vida do processo antes de ser encerrado (0s=infinite)")
	flag.Parse()
}

func main() {
	if consume {
		if declareQueue {
			if err := Declare(); err != nil {
				log.Printf("can`t declare queue or exchange, err: %s", err.Error())
			}
		}

		typeChannels := map[eventcounter.EventType]chan eventcounter.MessageConsumed{
			eventcounter.EventCreated: make(chan eventcounter.MessageConsumed),
			eventcounter.EventDeleted: make(chan eventcounter.MessageConsumed),
			eventcounter.EventUpdated: make(chan eventcounter.MessageConsumed),
		}

		ctx, _ := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		go func() {
			if err := Consume(ctx, typeChannels, wg); err != nil {
				log.Printf("can't consume any message, err: %s", err.Error())
			}
		}()

		go func() {
			for created := range typeChannels[eventcounter.EventCreated] {
				fmt.Println("created", created)
			}
		}()

		go func() {
			for deleted := range typeChannels[eventcounter.EventDeleted] {
				fmt.Println("deleted", deleted)
			}
		}()

		go func() {
			for updated := range typeChannels[eventcounter.EventUpdated] {
				fmt.Println("updated", updated)
			}
		}()

		time.Sleep(10 * time.Second)
		wg.Wait()
		//for _, ch := range msgs {
		//	if count {
		//		Write(outputDir, ch)
		//	}
		//}

		//cancel()
	}
}
