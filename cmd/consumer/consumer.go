package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	amqp091 "github.com/rabbitmq/amqp091-go"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	QueueName string = "eventcountertest"
	Key       string = "*.event.*"
)

var (
	conn    *amqp091.Connection
	channel *amqp091.Channel
)

func getChannel() (*amqp091.Channel, error) {
	var err error
	if channel != nil && !channel.IsClosed() {
		return channel, nil
	}

	conn, err = amqp091.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	go func() {
		log.Printf("Canal fechado, err: %s", <-channel.NotifyClose(make(chan *amqp091.Error, 2)))
	}()

	if channel.IsClosed() {
		return nil, errors.New("Canal fechado")
	}

	return channel, nil
}

func Declare() error {
	channel, err := getChannel()
	if err != nil {
		return err
	}

	if err := channel.ExchangeDeclare(amqpExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}

	log.Printf("Exchange declarada, declarando Queue %q", QueueName)

	queue, err := channel.QueueDeclare(QueueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("Queue: (%q %d messages, %d consumers), Exchange sendo escolhida (key %q)", "eventcountertest", queue.Messages, queue.Consumers, Key)

	if err := channel.QueueBind(QueueName, Key, amqpExchange, false, nil); err != nil {
		return err
	}

	log.Print("Queue da Exchange foi escolhida")

	return nil
}

func Consume(ctx context.Context) (map[string]chan eventcounter.MessageConsumed, error) {
	wg := sync.WaitGroup{}
	channel, err := getChannel()
	if err != nil {
		return nil, err
	}

	if channel.IsClosed() {
		return nil, errors.New("Canal está fechado")
	}

	done := make(chan error)
	SetupCloseHandler(channel, done)

	typeChannels := make(map[string]chan eventcounter.MessageConsumed)
	typeCounters := make(map[string]map[string]int)

	var timestamp int64
	for {
		msgs, err := channel.Consume(QueueName, "", true, false, false, false, nil)
		if err != nil {
			log.Printf("Falha ao receber mensagem do rabbitMQ: %s", err)
			select {
			case <-ctx.Done():
				timestamp = 0
				return typeChannels, nil
			default:
				time.Sleep(1 * time.Second)
				timestamp++
				if timestamp == 5 {
					if err := Shutdown(channel, done); err != nil {
						log.Fatalf("Erro ao desligar: %s", err)
					}
				}
				continue
			}
		}

		// Processar as mensagens em concorrência
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go handleMessage(msgs, done, typeChannels, typeCounters, &wg)
		}

		select {
		case <-ctx.Done():
			// Espera a queue fechar
			wg.Wait()
			return typeChannels, nil
		default:
		}
	}
	
	log.Println("Processamento finalizado.")
	// Esperar todas as rotinas terminarem

	return typeChannels, nil
}

func handleMessage(msgs <-chan amqp091.Delivery, done chan error, typeChannels map[string]chan eventcounter.MessageConsumed, typeCounters map[string]map[string]int, wg *sync.WaitGroup) {
	defer wg.Done()

	cleanup := func() {
		log.Printf("Fechar canal das mensagens")
		done <- nil
	}

	defer cleanup()

	for msg := range msgs {

		var message *eventcounter.MessageConsumed
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("Falha ao decodificar a mensagem: %s", err)
			continue
		}

		message.User = msg.RoutingKey[0:strings.Index(msg.RoutingKey, ".")]
		message.EventType = msg.RoutingKey[strings.LastIndex(msg.RoutingKey, ".")+1:]
		mapMutex := sync.RWMutex{}
		if processed, ok := typeCounters[message.EventType][message.Id]; !ok {
			// Se a mensagem não foi processada ainda, ela é enviada para o canal correspondente
			if _, ok := typeChannels[message.EventType]; !ok {
				// Criar um novo canal para este tipo de evento
				typeChannels[message.EventType] = make(chan eventcounter.MessageConsumed, 0)
				typeCounters[message.EventType] = make(map[string]int)
			}
			typeChannels[message.EventType] <- *message
			mapMutex.Lock()
			typeCounters[message.EventType][message.Id] = processed
			mapMutex.Unlock()
		}
	}
}

func SetupCloseHandler(channel *amqp091.Channel, done chan error) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Ctrl+C pressionado no Terminal")
		if err := Shutdown(channel, done); err != nil {
			log.Fatalf("Erro ao desligar: %s", err)
		}
		os.Exit(0)
	}()
}

func Shutdown(channel *amqp091.Channel, done chan error) error {
	if err := channel.Cancel("", true); err != nil {
		return fmt.Errorf("Cancelamento do consumer falhou: %s", err)
	}

	if err := channel.Close(); err != nil {
		return fmt.Errorf("AMQP erro de conexão: %s", err)
	}

	defer log.Printf("AMQP desligando OK")

	// espera pelo handleMessage() sair
	return <-done
}
