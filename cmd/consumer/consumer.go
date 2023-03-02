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

func Consume(ctx context.Context, typeChannels map[eventcounter.EventType]chan eventcounter.MessageConsumed, wg *sync.WaitGroup) error {
	channel, err := getChannel()
	if err != nil {
		return err
	}

	if channel.IsClosed() {
		return errors.New("Canal está fechado")
	}

	SetupCloseHandler(channel)

	typeCounters := make(map[eventcounter.EventType]map[string]int)
	mt := &sync.RWMutex{}
	msgs, err := channel.Consume(QueueName, "", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	for {
		select {
		case <-ctx.Done():
			cancel()
			return nil
		case msg := <-msgs:
			log.Print(msg)
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
			wg.Add(1)
			go handleMessage(msg, typeChannels, typeCounters, mt, wg)
		default:
		}
	}
}

func handleMessage(msg amqp091.Delivery, typeChannels map[eventcounter.EventType]chan eventcounter.MessageConsumed, typeCounters map[eventcounter.EventType]map[string]int, mt *sync.RWMutex, wg *sync.WaitGroup) {
	defer wg.Done()
	var message *eventcounter.MessageConsumed
	if err := json.Unmarshal(msg.Body, &message); err != nil {
		log.Printf("Falha ao decodificar a mensagem: %s", err)
		return
	}

	message.User = msg.RoutingKey[0:strings.Index(msg.RoutingKey, ".")]
	message.EventType = eventcounter.EventType(msg.RoutingKey[strings.LastIndex(msg.RoutingKey, ".")+1:])

	if processed, ok := typeCounters[message.EventType][message.Id]; !ok {
		// Se a mensagem não foi processada ainda, ela é enviada para o canal correspondente
		mt.Lock()
		if _, ok := typeChannels[message.EventType]; !ok {
			// Criar um novo canal para este tipo de evento
			typeChannels[message.EventType] = make(chan eventcounter.MessageConsumed, 0)
		}

		if _, ok := typeCounters[message.EventType]; !ok {
			typeCounters[message.EventType] = make(map[string]int)
		}

		ch := typeChannels[message.EventType]
		mt.Unlock()
		ch <- *message

		mt.Lock()
		typeCounters[message.EventType][message.Id] = processed
		mt.Unlock()
	}
}

func SetupCloseHandler(channel *amqp091.Channel) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Ctrl+C pressionado no Terminal")
		if err := Shutdown(channel); err != nil {
			log.Fatalf("Erro ao desligar: %s", err)
		}
		os.Exit(0)
	}()
}

func Shutdown(channel *amqp091.Channel) error {
	if err := channel.Cancel("", true); err != nil {
		return fmt.Errorf("Cancelamento do consumer falhou: %s", err)
	}

	if err := channel.Close(); err != nil {
		return fmt.Errorf("AMQP erro de conexão: %s", err)
	}

	defer log.Printf("AMQP desligando OK")

	// espera pelo handleMessage() sair
	return nil
}
