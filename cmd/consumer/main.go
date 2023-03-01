package main

import (
	"context"
	"flag"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
	"log"
	"sync"
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
		ctx, cancel := context.WithCancel(context.Background())

		msgs, err := Consume(ctx)
		if err != nil {
			log.Printf("can't consume any message, err: %s", err.Error())
		}

		var wg sync.WaitGroup
		for _, ch := range msgs {
			wg.Add(1)
			go func(channel chan eventcounter.MessageConsumed) {
				defer wg.Done()
				if count {
					Write(outputDir, channel)
				}
			}(ch)
		}

		wg.Wait()
		cancel()
	}
}
